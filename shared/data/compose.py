from shared.data.update import UpdateModel


class Composable(UpdateModel):
    __slots__ = ('_resultSet', '_needsUpdate')
    
    def __init__(self, *args, **kwargs):
        # Initialize mixins
        super(Composable, self).__init__(*args, **kwargs)
        self._needsUpdate = True
        
    def updateFirst(function):
        @functools.wraps(function)
        def ensureUpdated(self, *args, **kwargs):
            if self._needsUpdate:
                self.apply()
            return function(self, *args, **kwargs)
        return ensureUpdated
    
    @updateFirst
    def __iter__(self):
        return (group for group in self.results)

    @updateFirst
    def __getitem__(self, selector):
        return self._resultSet[selector]

    @property
    @updateFirst
    def results(self):
        return self._resultSet   
        
    def update(self, oldSelector, newSelector):
        self._needsUpdate = True
          
    def apply(self):
        for source in self.sources:
            if isinstance(source, Composable):
                source.apply()
        if self._needsUpdate:
            self._apply()
            
    def _apply(self):
        raise NotImplementedError("The base composable class' apply() must be overridden.")