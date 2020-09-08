from shared.data.compat import WeakSet, property


class UpdateModel(object):
    """Provide the object with a way to notify other objects
      that depend on it.
    """
    
    # Slots ensures we're explicit and fast
    __slots__ = ('_sources', '_listeners')
    
    def __init__(self, *args, **kwargs):
        """Initialize the chain.
        By tracking both sources and listeners, we can make a graph of what
          gets updated by what.
        """
        # Initialize mixins - NOPE Update is not cooperative.
        # It expects to be a base class
        #super(UpdateModel, self).__init__(*args, **kwargs)
        self._sources = tuple()
        self._listeners = WeakSet()
        
    def subscribe(self, listener):
        """Add a listener to the subscriber list.
        This isn't a set - order will likely help efficiency,
          the list will be updated infrequently, and the list
          should never get very big anyhow.
        Note that Calc objects have a source to act as their publisher list.
          (In case we want to backtrace.)
        """
        if not listener in self._listeners:
            self._listeners.add(listener)
    
    def unsubscribe(self, listener):
        """Remove a listener from the subscriber list.
        """
        while listener in self._listeners:
            self._listeners.remove(listener)
    
    def notify(self, oldSelector, newSelector):
        """Fires an update to make sure dependents are updated, if needed.
        The selectors show what happened in the update.
        """
        for dependent in self._listeners:
            try:
                dependent.update(oldSelector, newSelector)
            except NotImplementedError:
                pass
            except AttributeError:
                pass
            
    def update(self, oldSelector, newSelector):
        """Execute the update. Each class will have its own way to implement this."""
        # (None, None) signals that the data is out of date, 
        #  but there is nothing for dependents to do yet.
        #self._needsUpdate = True
        # Pass-through updates without triggering
        self.notify(None,None)
    
    @property
    def listeners(self):
        return self._listeners
    
    @listeners.setter
    def listeners(self, newListeners):
        self._replace_listeners(newListeners)
    
    def _replace_listeners(self, newSources):        
        """If the listeners are changed en masse, break
           all the subscriptions.
           This setter makes sure the subscription methods are never skipped.
        """
        while self._listeners:
            listener = self._listeners[0]
            self.unsubscribe(listener)
            
        for listener in newListeners:
            self.subscribe(listener)
        
    @property
    def sources(self):
        return self._sources
    
    @sources.setter
    def sources(self, newSources):
        self._replace_sources(newSources)
    
    def _replace_sources(self, newSources):
        for source in set(self._sources).difference(set(newSources)):
            source.unsubscribe(self)
        for source in newSources:
            source.subscribe(self)
        self._sources = newSources
