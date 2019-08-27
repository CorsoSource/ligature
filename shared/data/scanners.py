def passthrough(*args):
    return args

class Scanner(object):
    """An iterator that holds the position of a cursor while it scans.
    This way, an incompletely scanned recordset can resume once another that's
      zip()'d with it gets more data.
    """
    __slots__ = ('source', 'getter', 
                 '_group_cursor', '_record_cursor')
    
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

    def _iterGroup(self, group):
        try:
            for record in group[self._record_cursor:]:
                # records in groups never get appended, so ensure it never gets over
                self._record_cursor += 1 
                yield record
        finally:
            # In case of incomplete iteration, clean up
            if self._record_cursor == len(group): 
                self._record_cursor= 0
            else:
                self._group_cursor -= 1
    
    @property
    def exhausted(self):
        return self._record_cursor == 0 and self._group_cursor == len(self.source._groups)
    
    def __repr__(self):
        return '%s on group %d and record %d' % (type(self), self._group_cursor, self._record_cursor)
    
    def rewind(self, steps=1):
        raise NotImplementedError("The base scanner class' rewind(steps) must be overridden. This allows accidental consumption to be undone.")
        
    def __iter__(self):
        raise NotImplementedError("The base scanner class' __iter__() must be overridden.")
    
        
        
class ElementScanner(Scanner):
    def __iter__(self):      
        for group in self.source._groups[self._group_cursor:]: # error here merely stops iteration
            self._group_cursor += 1
            for record in self._iterGroup(group):
                yield self.getter(record)

    def rewind(self, steps=1):
        """Go back the given number of steps in the iteration."""
        while steps > 0:
            steps -= 1
            # If the record cursor is nonzero, back it off by one
            if self._record_cursor > 0:
                # remember, the group cursor aggressively iterates
                if self._group_cursor >= len(self.source._groups):
                    self._group_cursor = len(self.source._groups)-1 
                else:
                    self._record_cursor -= 1
            # If the record cursor is at the start rewind the group
            elif self._group_cursor:
                if self._group_cursor >= len(self.source._groups):
                    self._group_cursor = len(self.source._groups)-1 
                else:
                    self._group_cursor -= 1
                self._record_cursor = len(self.source._groups[self._group_cursor]) -1
            else: # already at the start
                return
                

class ChunkScanner(Scanner):
    """For a field in a source, this emits values in chunks
       - one chunk for each group in the source.
    """
    def __iter__(self):
        for group in self.source._groups[self._group_cursor:]: # error here merely stops iteration
            self._group_cursor += 1
            yield tuple(self.getter(record) for record in group)
            
    def rewind(self, steps=1):
        """Go back the given number of steps in the iteration."""
        while steps > 0:
            steps -= 1
            # If we have a nonzero group cursor, then just decrement it
            if self._group_cursor > 0:
                self._group_cursor -= 1
            else: # already at the start
                return


class RecordScanner(ElementScanner):
    """Returns the whole record when emitting."""
    def __iter__(self):
        for group in self.source._groups[self._group_cursor:]: # error here merely stops iteration
            self._group_cursor += 1
            for record in self._iterGroup(group):
                yield record


class GroupScanner(ChunkScanner):
    """Returns the whole group when emitting."""
    def __iter__(self):
        for group in self.source._groups[self._group_cursor:]:
            self._group_cursor += 1
            yield group

                
class ReplayingScanner(Scanner):
    """Continues to yield from the same place until ready is called."""
    
    __slots__ = ('_group_anchor', '_record_anchor', 
                 '_iterating_group', '_iterating_record')
    
    def __init__(self, *args, **kwargs):
        self._iterating_group = False
        self._iterating_record = False
        super(ReplayingScanner, self).__init__(*args, **kwargs)
    
    def __iter__(self):
        try:
            self._iterating_group = True
            for value in super(ReplayingScanner, self).__iter__():
                yield value 
        finally:
            self._iterating_group = False
    
    def _iterGroup(self, group):
        try:
            self._iterating_record = True
            for record in super(ReplayingScanner, self)._iterGroup(group):
                yield record
        finally:
            self._iterating_record = False
        
    def reset(self):
        super(ReplayingScanner, self).reset()
        self.anchor()
        
    def anchor(self):
        """Moves the last acknowledged position to the current."""
        # Cleanup after iteration means this function can see
        #   two types of values: one while iterating and one statically.
        if self._iterating_group and self._iterating_record:
            self._group_anchor = self._group_cursor - 1
        else:
            self._group_anchor = self._group_cursor
        
        self._record_anchor = self._record_cursor


class ReplayingElementScanner(ReplayingScanner, ElementScanner):
    """Emits a record's field data until exhausted like a normal generator,
       but if the ratchet isn't set, the next call to the generator
       will simply resume from where the last ratchet was.
    """
    def __iter__(self):
        self._record_cursor = self._record_anchor
        self._group_cursor = self._group_anchor
            
        return super(ReplayingElementScanner,self).__iter__()

    
class ReplayingChunkScanner(ReplayingScanner, ChunkScanner):
    """Emits groups until exhausted like a normal generator,
       but if the ratchet isn't set, the next call will 
       repeat from where the last ratchet was.
    """
    def __iter__(self):
        self._group_cursor = self._group_anchor
        return super(ReplayingChunkScanner,self).__iter__()


class ReplayingRecordScanner(ReplayingScanner, RecordScanner):
    """Emits a record's field data until exhausted like a normal generator,
       but if the ratchet isn't set, the next call to the generator
       will simply resume from where the last ratchet was.
    """
    def __iter__(self):
        self._record_cursor = self._record_anchor
        self._group_cursor = self._group_anchor
            
        return super(ReplayingRecordScanner,self).__iter__()


class ReplayingGroupScanner(ReplayingScanner, GroupScanner):
    """Emits groups until exhausted like a normal generator,
       but if the ratchet isn't set, the next call will 
       repeat from where the last ratchet was.
    """
    def __iter__(self):
        self._group_cursor = self._group_anchor
        return super(ReplayingGroupScanner,self).__iter__()        