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
        return (record._tuple[self._index]
                for record 
                in self._source._records)
    
    def __getitem__(self, slicer):
        """Returns a group or a slice of groups, where the selected column's value in the record is returned.
        """
        if isinstance(slicer, int):
            return (record._tuple[self._index] 
                    for record
                    in self._source._groups[slicer])
        else:
            if slicer.step:
                raise NotImplementedError("Columns should not be sliced by steps. Window the RecordSet and group records instead.")
            return (tuple(record._tuple[self._index]
                          for record
                          in group )
                    for group 
                    in islice(self._source._groups,slicer.start,slicer.stop) )

    def __repr__(self):
        'Format the representation string for better printing'
        return 'RecordSetColumn("%s" at %d)' % (self._source._RecordType._fields[self._index], self._index)
        

class RecordSet(object):
    """Holds groups of records. The gindex is the label for each of the tuples of Records.
    
    Based on collections.MutableSequence
    """
    
    __slots__ = ('_RecordType', '_groups', '_gindex', '_columns', '_subscribers')

    def _initializeDataSet(self, dataset, validate=False):
        """Convert the DataSet type into a RecordSet
        """
        self._RecordType = genRecordType(dataset.getColumnNames())
        columnIxs = range(len(self._RecordType._fields))
        records = []
        for rix in range(dataset.getRowCount()):
            row = tuple(dataset.getValueAt(rix, cix) for cix in columnIxs)
            records.append(self._RecordType(row))
        self._groups = [tuple(records)]
        
    
    def _initializeEmpty(self, RecordType):
        """Simply define what kind of RecordSet this will be, but start with no data.
        """
        self._RecordType = RecordType
        self._groups = []
        
    
    def _initializeRaw(self, RecordType, data):
        """Make a list with a single tuple entry, that was the list of new records.
           Using a generator as the tuple argument is about 4-10x slower.
        """
        self._RecordType = RecordType
        self._groups = [tuple([RecordType(record) 
                               for record 
                               in data])]
    
    
    def _initializeRecords(self, records, validate=False):
        """Initialize RecordSet from the records provided.
        Optionally validate the list to ensure they're all the same kind of record.
        """
        assert len(records) > 0, """If only records are provided, an example entry must be included: `len(records) == 0`"""
        self._RecordType = type(records[0])
        if validate:
            assert all(isinstance(r, RecordType) for r in records), 'All entries were not the same RecordType'
        self._groups = [tuple(records)]
    
    
    def __init__(self, initialData=None,  recordType=None, initialLabel=None, validate=False):        
        """When creating a new RecordSet, the key is to provide an unambiguous RecordType,
             or at least enough information to define one.
        """
        # We can initialize with a record type, a record, or an iterable of records
        # First check if it's a DataSet object. If so, convert it.
        if isinstance(initialData, BasicDataset):
            self._initializeDataSet(initialData)
        elif recordType:
            # create a RecordType, if needed
            if not isinstance(recordType, RecordType):
                recordType = genRecordType(recordType)
            if initialData:
                self._initializeRaw(recordType, initialData)
            else:
                self._initializeEmpty(recordType)
        elif initialData:
            self._initializeRecords(initialData, validate)
        else:
            raise ValueError("""Insufficient information to initialize the RecordSet."""
                             """ A RecordType must be implied by the constructor arguments.""")
        self._subscribers = []
        
        # monkey patch for higher speed access
        self._columns = tuple(RecordSetColumn(self, ix) for ix in range(len(self._RecordType._fields)))

        
    def coerceRecordType(self, record):
        return self._RecordType(record)
 
    
    # Sized
    def __len__(self):
        """Not terribly useful - this only tells how many chunks there are in the RecordSet.
           Use this as a poor man's fast cardinality check against other RecordSets.
        """
        return len(self._groups)
    
    # Iterable 
    # Sequence
    def __getitem__(self, index):
        return self._groups[index]

    def __iter__(self):
        return (recordGroup for recordGroup in self._groups)

    # Container
    def __contains__(self, search):
        """Search from the start for the search object.
           If a record is provided, then the groups will themselves be 
             exhaustively searched.
        """
        if isinstance(search, self._RecordType):
            for group in self._groups:
                if search in group:
                    return True
        elif search in self._groups:
            return True
        return False

    def __reversed__(self):
        """Reverses BOTH the groups and the records in the groups.
           Pure generators are returned.
        """
        return (reversed(group) for group in reversed(self._groups))
                
    def index(self, search):
        """Returns the index of the group the search item is found in."""
        if isinstance(search, self._RecordType):
            for groupIx, group in self._groups:
                for record in group:
                    if search == record:
                        return groupIx
        else:
            for groupIx, group in self._groups:
                if search == group:
                    return groupIx
        raise ValueError('Search item %r was not found' % search)

    def count(self, search):
        """Returns the number of times the search item is found.
           Note that this tests for equivalency, not instantiation!
        """
        if isinstance(search, self._RecordType):
            return sum(1
                       for group in self._groups
                       for record in group
                       if record == search )
        else:
            return sum(1 for group in self._groups if search == group)

    def append(self, addition):
        """Append the group of records to the end of _groups.
           If a record is given, an group of one will be appended.
        """
        if isinstance(addition, self._RecordType):
            self._groups.append(tuple(self.coerceRecordType(addition),))
        elif isinstance(addition, RecordSet):
            self.extend(addition)
        else: # tuple creation is slightly faster if the generator is consumed by a list first
            self._groups.append(tuple([self.coerceRecordType(entry)
                                       for entry
                                       in addition ]))
        self.notify(None, -1)
    
    def extend(self, additionalGroups):
        """Extend the records by concatenating the record groups of another RecordSet.
        """
        if isinstance(additionalGroups, RecordSet):
            assert self._RecordType._fields == recordSet._RecordType._fields, 'RecordSets can only be extended by other RecordSets of the same RecordType.'
            self._groups.extend(recordSet._groups)
        else:
            for group in additionalGroups:
                self.append(group)
        self.notify(None, slice(-len(additionalGroups),None))
    
    def __iadd__(self, addition):
        """Overload the shorthand += for convenience."""
        if isinstance(addition, self._RecordType):
            self.append(addition)
        else:
            self.extend(addition)
        return self
    
    def __getitem__(self, selector):
        """Get a particular group if an int or slice is provided. 
           If another identifier is provided, assume it's a column and return that.
        """
        if isinstance(selector, (int,slice)):
            return self._groups[selector]
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
    
    @property
    def _records(self):
        return (record 
                for group in self._groups 
                for record in group)
               
    def __repr__(self, ellideLimit=10):
        'Format the representation string for better printing'
        records = list(islice((r for r in self._records), ellideLimit))
        totalRecordCount = sum(len(g) for g in self._groups)
        out = ['RecordSet with %d groups of %d records' % (len(self), totalRecordCount)]
        # preprocess
        maxWidths = [max([len(f)] + [len(repr(v))+1 for v in column]) 
                     for f,column 
                     in zip(self._RecordType._fields,
                            zip(*records) 
                               if records 
                               else ['']*len(self._RecordType._fields))]
        digits = math.floor(math.log10(ellideLimit))
        prefixPattern = ' %%%ds  %%%ds |' % (digits, digits)
        recordPattern = prefixPattern + ''.join(' %%%ds' % mw for mw in maxWidths)
        out += [recordPattern % tuple(['',''] + list(self._RecordType._fields))]
        out += [recordPattern % tuple(['',''] + ['-'*len(field) for field in self._RecordType._fields])]
               
        for i,group in enumerate(self._groups):
            for j,record in enumerate(group):
                out += [recordPattern % tuple(['' if j else i,j] + [repr(v) for v in record])]
        if totalRecordCount > ellideLimit:
            out += ['  ... and %d ellided' % (totalRecordCount - ellideLimit)]   
        return '\n'.join(out)