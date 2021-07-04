from ligature.scanner import Scanner


class Identity(Scanner):
    """An identity scanner - it always returns the source object.
    If a field is provided, it will instead return that attribute of the object    
    """
    __slots__ = ('_max_iteration_page',)
    
    def __init__(self, source, field=None, max_iteration_page=1000):
        self._max_iteration_page = max_iteration_page
        self.source = source
        try:
            _ = getattr(source, field)
            self.getter = lambda myself, field=field: getattr(myself.source, field)
        except (AttributeError, TypeError):
            self.getter = lambda myself: myself.source
        
        self.reset()
            
    def rewind(self, steps=1):
        self.reset()
            
    def __iter__(self):
        for getted in self._iterGroup(None):
            yield getted
            
    def _iterGroup(self, group):
        self._pending_finally()        
        while not self.exhausted:
            yield self.getter(self)
            self._group_cursor += 1
    
    @property
    def exhausted(self):
        return not (self._max_iteration_page is None or self._group_cursor < self._max_iteration_page)
