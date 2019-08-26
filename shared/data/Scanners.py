class Scanner(object):
    """An iterator that holds the position of a cursor while it scans.
    This way, an incompletely scanned recordset can resume once another that's
      zip()'d with it gets more data.
    """
    __slots__ = ('source', 'getter', '_group_cursor', '_row_cursor')
    
    def __init__(self, source, field):
        self.source = source
        self.getter = source._RecordType.getGetter(field)
        self.reset() 
            
    def reset(self):
        # offset low for iteration restarts if more segments get added
        self._group_cursor = -1
        self._row_cursor = -1

    def _iterGroup(self, group):
        for record in group[self._row_cursor+1:]:
            self._row_cursor += 1
            yield record
    
    def __iter__(self):
        raise NotImplementedError("The base scanner class' __iter__() must be overridden.")
        
        
class RowScanner(Scanner):
    def __iter__(self):
        for group in self.source._groups[self._group_cursor+1:]: # error here merely stops iteration
            self._group_cursor += 1
            for record in self._iterGroup(group):
                yield self.getter(record)
            # after a group is iterated, return the row cursor to the beginning
            self._row_cursor = -1
                

class GroupScanner(Scanner):
    def __iter__(self):
        for group in self.source._groups[self._group_cursor+1:]: # error here merely stops iteration
            self._group_cursor += 1
            yield (self.getter(record) for record in group)
                
                
class ReplayingScanner(Scanner):
    """Continues to yield from the same place until ready is called."""
    
    __slots__ = ('_last_group', '_last_row')
    
    def reset(self):
        super(ReplayingScanner, self).reset()
        self.ratchet()
        
    def ratchet(self):
        """Moves the last acknowledged position to the current."""
        self._last_group = self._group_cursor
        self._last_row = self._row_cursor


class ReplayingRowScanner(RowScanner, ReplayingScanner):
    """Emits rows until exhausted like a normal generator,
       but if the ratchet isn't set, the next call to the generator
       will simply resume from where the last ratchet was.
    """
    def __iter__(self):
        self._row_cursor = self._last_row
        if self._last_row != -1:
            self._group_cursor = self._last_group - 1
        else:
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