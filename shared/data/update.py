class UpdateModel(object):
    """Provide the object with a way to notify other objects
      that depend on it.
    """
    
    # Slots ensures we're explicit and fast
    __slots__ = ('_sources', 'listeners')
    
    def __init__(self, *args, **kwargs):
        """Initialize the chain.
        By tracking both sources and listeners, we can make a graph of what
          gets updated by what.
        """
        self._sources = tuple()
        self.listeners = list()
        
    def subscribe(self, listener):
        """Add a listener to the subscriber list.
        This isn't a set - order will likely help efficiency,
          the list will be updated infrequently, and the list
          should never get very big anyhow.
        Note that Calc objects have a source to act as their publisher list.
          (In case we want to backtrace.)
        """
        if not listener in self.listeners:
            self.listeners.append(listener)
    
    def unsubscribe(self, listener):
        """Remove a listener from the subscriber list.
        """
        while listener in self.listeners:
            self.listeners.remove(listener)
    
    def notify(self, oldSelector, newSelector):
        """Fires an update to make sure dependents are updated, if needed.
        The selectors show what happened in the update.
        """
        for dependent in self.listeners:
            try:
                dependent.update(oldSelector, newSelector)
            except NotImplementedError:
                pass
            except AttributeError:
                pass
            
    def update(self, oldSelector, newSelector):
        """Execute the update. Each class will have its own way to implement this."""
        raise NotImplementedError
    
    # The sources tuple is set rarely, and to ensure the update model
    #   can't be forgotten, subscription is automatic now.
    @property
    def sources(self):
        return self._sources
    
    @sources.setter
    def sources(self, newSources):
        for source in set(self._sources).difference(set(newSources)):
            source.unsubscribe(self)
        for source in newSources:
            source.subscribe(self)
        self._sources = newSources