from weakref import WeakSet


class UpdateModel(object):
    """Provide the object with a way to notify other objects
      that depend on it.
    """
    
    # Slots ensures we're explicit and fast
    __slots__ = ('_sources', '_listeners', '__weakref__')
    
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
    
    def notify(self, old_selector, new_selector, source=None, depth=0):
        """Fires an update to make sure dependents are updated, if needed.
        The selectors show what happened in the update.
        """
        for dependent in self._listeners:
            try:
                dependent.update(old_selector, new_selector, source or self, depth+1)
            except NotImplementedError:
                pass
            except AttributeError:
                pass
            
    def update(self, old_selector, new_selector, source=None, depth=0):
        """Execute the update. Each class will have its own way to implement this."""
        # (None, None) signals that the data is out of date, 
        #  but there is nothing for dependents to do yet.
        #self._needsUpdate = True
        # Pass-through updates without triggering
        self.notify(old_selector, new_selector, source or self, depth)
    
    @property
    def listeners(self):
        return self._listeners
    
    @listeners.setter
    def listeners(self, new_listeners):
        self._replace_listeners(new_listeners)
    
    def _replace_listeners(self, new_listeners):        
        """If the listeners are changed en masse, break
           all the subscriptions.
           This setter makes sure the subscription methods are never skipped.
        """
        while self._listeners:
            listener = self._listeners[0]
            self.unsubscribe(listener)
            
        for listener in new_listeners:
            self.subscribe(listener)
        
    @property
    def sources(self):
        return self._sources
    
    @sources.setter
    def sources(self, new_sources):
        self._replace_sources(new_sources)
    
    def _replace_sources(self, new_sources):
        for source in set(self._sources).difference(set(new_sources)):
            source.unsubscribe(self)
        for source in new_sources:
            source.subscribe(self)
        self._sources = new_sources
