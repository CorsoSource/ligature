import functools

from ligature.update import UpdateModel
# from ligature.graph import GraphModel


class MetaComposable(type):
    
    registered = {}
    
    def __new__(cls, clsname, bases, attrs):
        newclass = super(MetaComposable, cls).__new__(cls, clsname, bases, attrs)
        cls.registered[clsname] = newclass
        return newclass
    
    
class Composable(UpdateModel):
    """All composable classes will scan and consume data
       as well as have a result that can be chained into the next.
    """
    __metaclass__ = MetaComposable
    __slots__ = ('scanners', '_resultset', '_awaiting_apply')
    
    def __init__(self, *args, **kwargs):
        self._awaiting_apply = True

        # Initialize mixins
        super(Composable, self).__init__(*args, **kwargs)
        
    def _debounce_scanners(self):
        """As we iterate the scanners, one may exhaust prematurely,
           leaving them in an unaligned state (the first scanners
           may have iterated while the stopped won't).
        """
        # Check if any scanners failed to finish or needs to be reprimed
        if all(scanner.exhausted for scanner in self.scanners):
            return
        for scanner in self.scanners:
            # Once the exhausted scanner is reached, stop. 
            # Only the previous would have over-emitted.
            if scanner.exhausted:
                break
            else:
                scanner.rewind()
                
    def _update_first(function):
        @functools.wraps(function)
        def ensure_updated(self, *args, **kwargs):
            if self._awaiting_apply:
                self.apply()
            return function(self, *args, **kwargs)
        return ensure_updated
    
    def __iter__(self):
        return iter(self.results)
    
    @property
    def groups(self):
        return self.results.groups

    @property
    def _groups(self):
        return self.results._groups

    @property
    def records(self):
        return self.results.records

    def __getitem__(self, selector):
        return self.results[selector]


    @property
    @_update_first
    def results(self):
        return self._resultset


    def clear(self):
        # TODO: this likely causes an extra update in the subscription graph
        #   Ensure that the UpdateModel only notifies once for a clear
        self._resultset.clear() # NOTE: we're depending on the recordset to notify of the update
        for scanner in self.scanners:
            scanner.reset()
        # mark dirty for update next call
        self._awaiting_apply = True


    # This allows us to better control how the graph is followed
    def _replace_sources(self, newSources):
        for source in set(self._sources).difference(set(newSources)):
            self._del_source(source)
        for source in newSources:
            self._add_source(source)

    def _add_source(self, new_source):
        try:
            if isinstance(new_source, Composable):
                new_source._resultset.subscribe(self)
            else:
                new_source.subscribe(self)
        except AttributeError:
            pass # source does not subscribe
        self._sources += (new_source,)

    def _del_source(self, old_source):
        old_source.unsubscribe(self)
        self._sources = tuple(s for s in self._sources if s is not old_source)


    def update(self, old_selector, new_selector, source=None, depth=0):
        # (None, None) signals that the data is out of date, 
        #  but there is nothing for dependents to do yet.
        # If the dependent has been fully replaced, from start to last, then reset
        self._awaiting_apply = True
        if source is not self and old_selector == slice(None, None) and new_selector == slice(None, None):
            self.clear()
        
        super(Composable, self).update(old_selector, new_selector, source or self, depth)
          
    def apply(self):
        for source in self.sources:
            if isinstance(source, Composable):
                source.apply()
        if self._awaiting_apply:
            self._apply()
        self._debounce_scanners()
        
    def _apply(self):
        raise NotImplementedError("The base composable class' apply() must be overridden.")


    def __repr__(self):
        return '<%s: %r>\n%r' % (type(self).__name__, self.metadata, self.results)