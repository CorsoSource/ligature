import functools, math
from itertools import izip as zip

from com.inductiveautomation.ignition.common import BasicDataset

from shared.data.RecordType import RecordType, genRecordType


class RecordSetColumn(object):
    __slots__ = ('_source', '_index')
    
    def __init__(self, recordSet, columnIndex):
        self._source = recordSet
        self._index = columnIndex

    def __iter__(self):
        """Redirect to the tuple stored when iterating."""
        return (record._tuple[self._index] for record in self._source._records)
    
    def __getitem__(self, slicer):
        """Return the value or a slice, depending.
        This has what I consider a nice balance in overhead. If you find that calling a RecordSet by column
          is more convenient, by index this way is just over twice as slow as RecordSet._records[index].column
          and the slice is about thrice as slow as that.
        """
        if isinstance(slicer, int):
            return self._source._records[slicer]._tuple[self._index]
        else:
            return (record._tuple[self._index] for record in self._source._records[slicer])

    def __repr__(self):
        'Format the representation string for better printing'
        return 'RecordSetColumn("%s" at %d)' % (self._source._RecordType._fields[self._index], self._index)


class RecordSet(object):
    """
    
    RecordSets cache the results of slicing, so if the same slice is requested,
      the value can be immediately be returned. If the cached slice gets invalidated,
      then we just need to delete the entry. Missing slices get recalculated anyhow.
    
    Based on collections.MutableSequence
    """
    
    __slots__ = ('_RecordType', '_records', '_subscribers')
    
    def __init__(self, *args):        
        if not args:
            raise ValueError('RecordSets require a RecordType.')
        
        # We can initialize with a record type, a record, or an iterable of records
        # First check if it's a DataSet object. If so, convert it.
        if len(args) == 1 and isinstance(args[0], BasicDataset):
            ds = args[0]
            self._RecordType = genRecordType(ds.getColumnNames())
            columnIxs = range(len(self._RecordType._fields))
            
            records = []
            for rix in range(ds.getRowCount()):
                row = tuple(ds.getValueAt(rix, cix) for cix in columnIxs)
                records.append(self._RecordType(row))
            self._records = records
        else:
            # If not, check if the first entry is the type,
            #   and if so initialize the records provided
            try:
                assert issubclass(args[0], RecordType) == True
                self._RecordType = args[0]
                if len(args) > 1:
                    # RecordSet(RecordType, iterable)
                    if len(args) == 2:
                        self._records = [self._RecordType(record) for record in args[1]]
                    # RecordSet(RecordType, *iterable)
                    else:
                        self._records = [self._RecordType(record) for record in args[1:]]
                # RecordSet(RecordType)
                else:
                    self._records = []
                
            except AssertionError, err:
                raise TypeError('Initialize RecordSets with RecordType objects only.\nVerify first argument is of RecordType and arguments have the correct number of values.')
            # Otherwise we're initializing with data
            except TypeError:
                # Check first if the data is just a record
                # RecordSet(someRecord)
                if isinstance(args, RecordType):
                    self._RecordType = type(args)
                # or if it's a bunch of records
                # RecordSet(*iterableOfRecords)
                elif isinstance(args[0], RecordType):
                    self._RecordType = type(args[0])
                    assert all(isinstance(r, RecordType) for r in args), 'All entries were not a RecordType'            
                else:
                    raise TypeError('RecordSets may contain RecordType objects only.')
            
                self._records = list(args)
            except Exception, err:
                raise err
        
        self._subscribers = []
        
        # monkey patch for higher speed access
        self._columns = tuple(RecordSetColumn(self, ix) for ix in range(len(self._RecordType._fields)))

    def coerceRecordType(self, record):
        if not isinstance(record, self._RecordType):
            return self._RecordType(record)
        else:
            return record    
    
    # Sized
    def __len__(self):
        return len(self._records)
    
    # Iterable 
    # Sequence
    def __getitem__(self, index):
        return self._records[index]

    def __iter__(self):
        return (record for record in self._records)

    # Container
    def __contains__(self, searchRecord):
        for record in self._records:
            if record == searchRecord:
                return True
        return False

    def __reversed__(self):
        return (record for record in reversed(self._records))
                
    def index(self, searchRecord):
        '''S.index(value) -> integer -- return first index of value.
           Raises ValueError if the value is not present.
        '''
        for i, record in enumerate(self._records):
            if record == searchRecord:
                return i
        raise ValueError

    def count(self, searchRecord):
        'S.count(value) -> integer -- return number of occurrences of value'
        return sum(1 for record in self._records if record == searchRecord)
        
    # MutableSequence
    def __setitem__(self, index, record):
        self._records[index] = self.coerceRecordType(record)
        self.notify(index, index)

    def __delitem__(self, index):
        del self._records[index]
        self.notify(index, None)

    def insert(self, index, record):
        'insert record before index'
        self._records.insert(index, self.coerceRecordType(record))
        self.notify(None, index)

    def append(self, record):
        'append record to the end of the stored records'
        self._records.append(self.coerceRecordType(record))
        self.notify(None, -1)
    
    def extend(self, records):
        'extend records by bulk appending the new records to the end'
        self._records.extend([self.coerceRecordType(record) for record in records])
        self.notify(None, slice(-len(records),None))
    
    def pop(self, index=-1):
        '''remove and return record at index (default last).
           Raise IndexError if records are empty or index is out of range.
        '''
        record = self._records[index]
        del self._records[index]
        try:
            return record
        finally: # don't preempt the caller by letting another cut in line!
            self.notify(index, None)

    def remove(self, record):
        '''remove first occurrence of record.
           Raise ValueError if the value is not present.
        '''
        index = self.index(record)
        del self._records[index]
        self.notify(index, None)
        
    def __iadd__(self, records):
        if isinstance(records, RecordType):
            self._records.append(records)
            self.notify(None, -1)
        else:
            self.extend(records)
        return self
    
    def __getitem__(self, selector):
        if isinstance(selector, (int,slice)):
            return self._records[selector]
        else:
            ix = self._RecordType._lookup[selector]
            return self._columns[ix]
        
    def notify(self, oldSelector=None, newSelector=None):
        """Fires an update to make sure dependents are updated, if needed"""
        for dependent in self._subscribers:
            try:
                dependent.update(self, oldSelector, newSelector)
            except AttributeError:
                pass

    def __repr__(self):
        'Format the representation string for better printing'
        ellideLimit = 10
        out = ['RecordSet with %s records' % len(self)]
        # preprocess
        maxWidths = [max([len(f)] + [len(repr(v))+1 for v in column]) 
                     for f,column 
                     in zip(self._RecordType._fields,
                            zip(*self._records[:ellideLimit]) 
                               if self._records 
                               else ['']*len(self._RecordType._fields))]        
        prefixPattern = ' %%%ds |' % math.floor(math.log10(ellideLimit))
        recordPattern = prefixPattern + ''.join(' %%%ds' % mw for mw in maxWidths)
        out += [recordPattern % tuple([''] + list(self._RecordType._fields))]
        out += [recordPattern % tuple([''] + ['-'*len(field) for field in self._RecordType._fields])]
        for i,record in enumerate(self._records[:ellideLimit]):
            out += [recordPattern % tuple([i] + [repr(v) for v in record])]
        if len(self._records) > ellideLimit:
            out += ['  ... and %d ellided' % (len(self._records) - ellideLimit)]   
        return '\n'.join(out)