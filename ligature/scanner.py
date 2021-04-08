def passthrough(*args):
    return args


class MetaScanner(type):
    
    registered = {}
    
    def __new__(cls, clsname, bases, attrs):
        newclass = super(MetaScanner, cls).__new__(cls, clsname, bases, attrs)
        cls.registered[clsname] = newclass
        return newclass
    

class Scanner(object):
    """An iterator that holds the position of a cursor while it scans.
    This way, an incompletely scanned recordset can resume once another that's
      zip()'d with it gets more data.
    """
    __metaclass__ = MetaScanner
    __slots__ = ('source', 'getter', 
                 '_group_cursor', '_record_cursor',
                 '_iterating_group', '_iterating_record',
                 )
    
    def __init__(self, source, field=None):
        self.source = source
        
        # allow Scanner to be a bit more generic while useful in the default
        if not field is None:
            self.getter = source._RecordType.getGetter(field)
        else: 
            self.getter = None
            
        self.reset() 
            
    def reset(self):
        # offset low for iteration restarts if more segments get added
        self._group_cursor = 0
        self._record_cursor = 0
        self._iterating_group = None
        self._iterating_record = None

    def _iterGroup(self, group):
        try:
            self._iterating_group = group
            for record in group[self._record_cursor:]:
                # records in groups never get appended, so ensure it never gets over
                self._record_cursor += 1
                yield record
        finally:
            self._iterGroup_finally()

    def _pending_finally(self):
        if self._iterating_group:
            self._iterGroup_finally()
        if self._iterating_record:
            self._iterRecord_finally()

    def _iterGroup_finally(self):
        # In case of incomplete iteration, clean up
        # Note that this is needed, since the finally clause in the iterGroup does NOT run
        #   but once PER ITERATION. That means if more than one scanner is zipped, only
        #   the first finally is run - the rest must know they stopped incompletely!
        if self._record_cursor == len(self._iterating_group):
            self._record_cursor= 0
        else:
            self._group_cursor -= 1
        self._iterating_group = None

    def _iterRecord_finally(self):
        pass    

    def __next__(self):
        return self.next()

    def next(self):
        return next(iter(self))

    @property
    def exhausted(self):
        return self._record_cursor == 0 and self._group_cursor == len(self.source._groups)
    
    def __repr__(self):
        return '%s on group %d and record %d' % (type(self), self._group_cursor, self._record_cursor)
    
    def rewind(self, steps=1):
        raise NotImplementedError("The base scanner class' rewind(steps) must be overridden. This allows accidental consumption to be undone.")
        
    def __iter__(self):
        raise NotImplementedError("The base scanner class' __iter__() must be overridden.")
