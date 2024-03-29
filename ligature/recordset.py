from ligature.record import RecordType, genRecordType
from ligature.update import UpdateModel
# from ligature.graph import GraphModel

import functools, math
from itertools import izip as zip
from itertools import islice

from weakref import WeakSet

try:
    from com.inductiveautomation.ignition.common import BasicDataset
except ImportError:
    from abc import ABCMeta
    class BasicDataset():
        __metaclass__ = ABCMeta
        pass


class RecordSetColumn(object):
    __slots__ = ('_source', '_index')
    
    def __init__(self, recordSet, columnIndex):
        self._source = recordSet
        self._index = columnIndex

    def __iter__(self):
        """Redirect to the tuple stored when iterating."""
        return ((record._tuple[self._index]
                  for record 
                  in group)
                for group in self._source._groups)
    
    def __getitem__(self, selector):
        """Returns a group or a slice of groups, where the selected column's value in the record is returned.
        """
        if isinstance(selector, slice):
            if selector.step:
                raise NotImplementedError("Columns should not be sliced by steps. Window the RecordSet and group records instead.")
            
            return ((record._tuple[self._index]
                      for record
                      in group )
                    for group 
                    in islice(self._source._groups, selector.start, selector.stop) )
        else:
            return (record._tuple[self._index]
                      for record
                      in self._source._groups[selector] )

    def __repr__(self):
        'Format the representation string for better printing'
        return 'RecordSetColumn("%s" at %d)' % (self._source._RecordType._fields[self._index], self._index)

        

class RecordSet(UpdateModel):
    """Holds groups of records. The gindex is the label for each of the tuples of Records.
    
    Based on collections.MutableSequence
    """
    # Track all the RecordSets that get made. We can use this to automagically trim the cache.
    # References need to be weak to ensure garbage collection can continue like normal.
    _instances = WeakSet()

    __slots__ = ('_RecordType', '_groups', '_columns')


    def __new__(cls, *args, **kwargs):
        """See https://stackoverflow.com/a/12102666/1943640 for an example of this."""
        instance = super(RecordSet, cls).__new__(cls, *args, **kwargs)
        cls._instances.add(instance)
        return instance

    @classmethod
    def _truncateAll(cls):
        instances = list(cls._instances)
        for instance in instances:
            instance.truncate()

    def truncate(self):
        """Clear out data that is not unused.
           Cooperate with the scanners pointing to this RecordSet
             by asking for a tuple declaring what records or groups
             it needs.
           Only groups are truncated, and only from the beginning.
           Once completed, each listening scanner is notified
             so its cursors can be corrected accordingly.
        """
        raise NotImplementedError

        safeGroupIx = -1
        for scanner in listeningScanners:
            pass

        for scanner in listeningScanners:
            scanner.updateCursorsForRemoval(safeGroupIx)


    # INIT
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

    def _initializeCopy(self, recordSet):
        self._RecordType = recordSet._RecordType
        self._groups = [group for group in recordSet._groups]
        # Note that it'll regenerate indexes even on copy...
        
    
    def __init__(self, initialData=None,  recordType=None, initialLabel=None, validate=False, scalar_tuples=False, *args, **kwargs):#, indexingFunction=None):        
        """When creating a new RecordSet, the key is to provide an unambiguous RecordType,
             or at least enough information to define one.
        """        
        # We can initialize with a record type, a record, or an iterable of records
        # First check if it's a DataSet object. If so, convert it.
        if isinstance(initialData, BasicDataset):
            self._initializeDataSet(initialData)
        elif recordType:
            # create a RecordType, if needed
            if not (isinstance(recordType, type) and issubclass(recordType, RecordType)):
                recordType = genRecordType(recordType, scalar_tuples=scalar_tuples)
            if initialData:
                self._initializeRaw(recordType, initialData)
            else:
                self._initializeEmpty(recordType)
        elif initialData:
            if isinstance(initialData, RecordSet):
                self._initializeCopy(initialData)
            else:
                self._initializeRecords(initialData, validate)
        else:
            raise ValueError("""Insufficient information to initialize the RecordSet."""
                             """ A RecordType must be implied by the constructor arguments.""")
                
        # monkey patch for higher speed access
        self._columns = tuple(RecordSetColumn(self, ix) 
                              for ix 
                              in range(len(self._RecordType._fields)))

        # Initialize mixins
        super(RecordSet, self).__init__(*args, **kwargs)
            
            
    def clear(self):
        self._groups = []
        self.notify(slice(None, None), slice(None, None),)
        
        
    def column(self,column):
        return self._columns[self._RecordType._lookup[column]]

    
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
    
    def __getitem__(self, selector):
        """Get a particular set of records given the selector.
           Note that this is record-centric, like the iterator.
        """
        if isinstance(selector, tuple):
            column,slicer = selector
            return self.column(column)[slicer]
        elif isinstance(selector, slice):
            if (selector.start or 0) < 0:
                return islice(self.records_reversed, 
                    selector.stop if selector.stop is None else -selector.stop, 
                    - selector.start, selector.step)
            else:
                return islice(self.records, selector.start, selector.stop, selector.step)     
        elif isinstance(selector, int):
            index = selector
            if index >= 0:
                for group in self._groups:
                    if len(group) > index:
                        return group[index]
                    index -= len(group)
                else:
                    raise IndexError("There are not enough records in the groups to meet the index %d" % selector)
            else:
                index *= -1 # make absolute to count backwards from
                for group in reversed(self._groups):
                    if len(group) >= index:
                        return group[len(group)-index]
                    index -= len(group)
                else:
                    raise IndexError("There are not enough records in the groups to meet back to the index %d" % selector)
        else:
            raise NotImplementedError("The selector '%r' is not implemented" % selector)
            #return self._groups[selector]
    
    @property
    def groups(self):
        return (group 
                for group 
                in self._groups)
    
    @property
    def records(self):
        return (record 
                for group in self._groups 
                for record in group)

    @property
    def records_reversed(self):
        return (record
                for group in reversed(self._groups)
                for record in reversed(group) )
    
    def __iter__(self):
        return self.records
        #return (recordGroup for recordGroup in self._groups)

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
                
    def index(self, search, fromTop=True):
        """Returns the index of the group the search item is found in."""
        if fromTop:
            iterDir = (gg for gg in reversed(enumerate(self._groups)))
        else:
            iterDir = (gg for gg in enumerate(self._groups))
            
        if isinstance(search, self._RecordType):
            for gix, group in iterDir:
                if record in group:
                    return gix
        elif isinstance(search, tuple):
            for gix, group in iterDir:
                if search == group:
                    return gix
                for record in group:
                    if record._tuple == search:
                        return gix
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
        elif isinstance(search, tuple):
            return sum(1 for group in self._groups if search == group)
        return 0

    def append(self, addition):
        """Append the group of records to the end of _groups.
           If a record is given, an group of one will be appended.
        """
        if isinstance(addition, RecordSet):
            self.extend(addition)
        else:
            if isinstance(addition, self._RecordType):
                newGroup = (self.coerceRecordType(addition),)
            else: 
                # tuple creation is slightly faster if the generator is consumed by a list first
                newGroup = tuple([self.coerceRecordType(entry)
                                  for entry
                                  in addition ])
            self._groups.append(newGroup)
            # signal that a new group was added
            self.notify(None, slice(-1, None))
    
    def extend(self, additionalGroups):
        """Extend the records by concatenating the record groups of another RecordSet.
        """
        if isinstance(additionalGroups, RecordSet):
            assert self._RecordType._fields == additionalGroups._RecordType._fields, 'RecordSets can only be extended by other RecordSets of the same RecordType.'
            self._groups.extend(additionalGroups._groups)
            self.notify(None, slice(-len(additionalGroups),None))
        else:
            for group in additionalGroups:
                self.append(group)
    
    def __iadd__(self, addition):
        """Overload the shorthand += for convenience."""
        if isinstance(addition, self._RecordType):
            self.append(addition)
        else:
            self.extend(addition)
        return self
    
    def _default_graph_attributes(self):
        fields = ', '.join(self._RecordType._fields)
        label = 'RecordSet\n%s' % fields
        return {
            'label': label,
            'shape': 'cylinder',
        }
    
    def __str__(self):
        return 'RecordSet=%r' % repr(self._RecordType._fields)
               
    def __repr__(self, elideLimit=20, tailCount=None, indent=''):
        'Format the representation string for better printing'
        records = list(islice((r for r in self.records), elideLimit))
        totalRecordCount = sum(len(g) for g in self.groups)
        out = ['RecordSet: %d groups of %d records%s' % (
                    len(self), totalRecordCount, ' \n%s     meta: %r' % (indent, self.metadata,) if self.metadata else '')]
        # preprocess
        maxWidths = [max([len(f)] + [len(repr(v))+1 for v in column]) 
                     for f,column 
                     in zip(self._RecordType._fields,
                            zip(*records) 
                               if records 
                               else ['']*len(self._RecordType._fields))]
        digits = math.floor(math.log10(elideLimit or 1)) + 1
        maxGixWidth = math.floor(math.log10(len(self._groups) or 1)) + 1
        prefixPattern = ' %%%ds  %%%ds |' % (maxGixWidth, digits)
        recordPattern = prefixPattern + ''.join(' %%%ds' % mw for mw in maxWidths)
        out += [recordPattern % tuple(['',''] + list(self._RecordType._fields))]
        out += [recordPattern % tuple(['',''] + ['-'*len(field) for field in self._RecordType._fields])]
        
        # set the default; use 0 to force no tail block
        if tailCount is None:
            tailCount = elideLimit / 2

        if tailCount and totalRecordCount > elideLimit and elideLimit > tailCount:
            remaining = elideLimit - tailCount
        else:
            remaining = elideLimit

        # print the main block
        for gix,group in enumerate(self._groups):
            for j,record in enumerate(group):
                out += [recordPattern % tuple(['' if j else gix,j] + [repr(v) for v in record])]
                remaining -= 1
                if not remaining:
                    break
            if not remaining:
                break

        if totalRecordCount > elideLimit:
            # print an end block, if set
            if tailCount and elideLimit > tailCount:
                if totalRecordCount - elideLimit:
                    out += ['  ... (%d elided)' % (totalRecordCount - elideLimit)]

                out_tail = []

                groupCount = len(self._groups)
                for gix, group in enumerate(reversed(self._groups)):
                    gix = groupCount - gix - 1
                    recordCount = len(group)

                    for j, record in enumerate(reversed(group)):
                        j = recordCount - j - 1
                        out_tail += [recordPattern % tuple(['' if j else gix,j] + [repr(v) for v in record])]
                        tailCount -= 1
                        if not tailCount:
                            break
                    if not tailCount:
                        break
                out += list(reversed(out_tail))
            else:
                out += ['  ... and %d elided' % (totalRecordCount - elideLimit)]

        out += ['']

        out = [indent + line for line in out]

        return '\n' + '\n'.join(out) + '\n'

