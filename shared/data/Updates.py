class UpdateModel(object):
    __slots__ = ('sources', 'listeners')
    
    def __init__(self):
        self.sources = tuple()
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
            
    def notify(self, oldSelector, newSelector):
        """Fires an update to make sure dependents are updated, if needed.
        The selectors show what happened in the update.
        """
        for dependent in self.listeners:
            try:
                dependent.update(self, oldSelector, newSelector)
            except NotImplementedError:
                pass
            except AttributeError:
                pass
            
    def update(self, oldSelector, newSelector):
        """Execute the update. Each class will have its own way to implement this."""
        raise NotImplementedError