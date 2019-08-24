


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
        raise NotImplementedError        

        
class RowScanner(Scanner):
    def __iter__(self):
        for group in self.source._groups[self._group_cursor+1:]: # error here merely stops iteration
            self._group_cursor += 1
            self._row_cursor = -1
            for record in self._iterGroup(group):
                yield self.getter(record)
                
class GroupScanner(Scanner):
    def __iter__(self):
        for group in self.source._groups[self._group_cursor+1:]: # error here merely stops iteration
            self._group_cursor += 1
            yield (self.getter(record) for record in group)
                
                
class ReplayingScanner(Scanner):
    """Continues to yield from the same place until ready is called."""
    def ready(self):
        #increment the scanning index now
        raise NotImplementedError

class ReplayingRowScanner(ReplayingScanner):
    def __iter__(self):
        raise NotImplementedError
    def ready(self):
        raise NotImplementedError
    
class ReplayingGroupScanner(ReplayingScanner):
    def __iter__(self):
        raise NotImplementedError
    def ready(self):
        raise NotImplementedError
    
    