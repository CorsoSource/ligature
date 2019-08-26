class Scanner(object):
    """An iterator that holds the position of a cursor while it scans.
    This way, an incompletely scanned recordset can resume once another that's
      zip()'d with it gets more data.
    """
    __slots__ = ('source', 'getter', 
                 '_group_cursor', '_row_cursor'
                 '_exhausted')
    
    def __init__(self, source, field):
        self.source = source
        self.getter = source._RecordType.getGetter(field)
        self.reset() 
            
    def reset(self):
        # offset low for iteration restarts if more segments get added
        self._group_cursor = 0
        self._row_cursor = 0

    def _iterGroup(self, group):
        for record in group[self._row_cursor:]:
            # records in groups never get appended, so ensure it never gets over
            self._row_cursor += 1 
            if self._row_cursor == len(group): self._row_cursor= 0
            yield record
    
    @property
    def exhausted(self):
        return self._row_cursor == 0 and self._group_cursor == len(self.source._groups)
    
    def __repr__(self):
        return '%s on group %d and row %d' % (type(self), self._group_cursor, self._row_cursor)
    
    def rewind(self, steps=1):
        raise NotImplementedError("The base scanner class' rewind(steps) must be overridden. This allows accidental consumption to be undone.")
        
    def __iter__(self):
        raise NotImplementedError("The base scanner class' __iter__() must be overridden.")
        
        
class RowScanner(Scanner):
    def __iter__(self):      
        try:
            for group in self.source._groups[self._group_cursor:]: # error here merely stops iteration
                self._group_cursor += 1
                for record in self._iterGroup(group):
                    yield self.getter(record)
                # after a group is iterated, return the row cursor to the beginning
                self._row_cursor = 0
        finally:
            # incomplete iteration - roll back group cursor to realign row cursor
            if self._row_cursor and self._group_cursor:
                self._group_cursor -= 1
                
    def rewind(self, steps=1):
        """Go back the given number of steps in the iteration."""
        while steps > 0:
            steps -= 1
            # If the row cursor is nonzero, back it off by one
            if self._row_cursor > 0:
                # remember, the group cursor aggressively iterates
                if self._group_cursor >= len(self.source._groups):
                    self._group_cursor = len(self.source._groups)-1 
                else:
                    self._row_cursor -= 1
            # If the row cursor is at the start rewind the group
            elif self._group_cursor:
                if self._group_cursor >= len(self.source._groups):
                    self._group_cursor = len(self.source._groups)-1 
                else:
                    self._group_cursor -= 1
                self._row_cursor = len(self.source._groups[self._group_cursor]) -1
            else: # already at the start
                return
                

class GroupScanner(Scanner):
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
                
                
class ReplayingScanner(Scanner):
    """Continues to yield from the same place until ready is called."""
    
    __slots__ = ('_last_group', '_last_row')
    
    def reset(self):
        super(ReplayingScanner, self).reset()
        self.ratchet()
        
    def ratchet(self):
        """Moves the last acknowledged position to the current."""
        # Note that the cursors are preemptively incremented
        if self._group_cursor:
            # did we finish iteration of that group? If so jump to next
            if self._row_cursor == 0 or self._row_cursor == len(self.source._groups[self._group_cursor-1]):
                self._last_group = self._group_cursor
                self._last_row = 0
            else:
                self._last_group = self._group_cursor - 1
                self._last_row = self._row_cursor
        # Reset()
        else:
            self._last_group = self._group_cursor
            self._last_row = self._row_cursor


class ReplayingRowScanner(RowScanner, ReplayingScanner):
    """Emits rows until exhausted like a normal generator,
       but if the ratchet isn't set, the next call to the generator
       will simply resume from where the last ratchet was.
    """
    def __iter__(self):
        self._row_cursor = self._last_row
        self._group_cursor = self._last_group
            
        return super(ReplayingRowScanner,self).__iter__()

    
class ReplayingGroupScanner(GroupScanner, ReplayingScanner):
    """Emits groups until exhausted like a normal generator,
       but if the ratchet isn't set, the next call will 
       repeat from where the last ratchet was.
    """
    def __iter__(self):
        self._group_cursor = self._last_group
        return super(ReplayingGroupScanner,self).__iter__()              