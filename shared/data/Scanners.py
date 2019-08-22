


class Scanner(object):
    """An iterator that holds the position of a cursor while it scans.
    This way, an incompletely scanned recordset can resume once another that's
      zip()'d with it gets more data.
    """
    __slots__ = ('source', 'getter', '_gcursor', '_rcursor')
    
    def __init__(self, source, field):
        self.source = source
        self.getter = source._RecordType.getGetter(field)
        self.reset() 
    
    def _iterGroup(self, group):
        for record in group[self._rcursor+1:]:
            self._rcursor += 1
            yield record
    
    def __iter__(self):
        for group in self.source._groups[self._gcursor+1:]: # error here merely stops iteration
            self._gcursor += 1
            self._rcursor = -1
            for record in self._iterGroup(group):
                yield self.getter(record)
            
    def reset(self):
        # offset low for iteration restarts if more segments get added
        self._gcursor = -1
        self._rcursor = -1
    