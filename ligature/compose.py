import functools

from ligature.update import UpdateModel
from ligature.graph import GraphModel


class MetaComposable(type):
    
    registered = {}
    
    def __new__(cls, clsname, bases, attrs):
        newclass = super(MetaComposable, cls).__new__(cls, clsname, bases, attrs)
        cls.registered[clsname] = newclass
        return newclass
    
    
class Composable(GraphModel,UpdateModel):
    """All composable classes will scan and consume data
       as well as have a result that can be chained into the next.
    """
    __metaclass__ = MetaComposable
    __slots__ = ('scanners', '_resultSet', '_awaiting_apply')
    
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
    
    @_update_first
    def __iter__(self):
        return (group for group in self.results.groups)

    @_update_first
    def __getitem__(self, selector):
        return self._resultSet[selector]

    @property
    @_update_first
    def results(self):
        return self._resultSet   

    # This allows us to better control how the graph is followed
    def _replace_sources(self, newSources):
        for source in set(self._sources).difference(set(newSources)):
            source.unsubscribe(self)
        for source in newSources:
            if isinstance(source, Composable):
                source._resultSet.subscribe(self)
            else:
                source.subscribe(self)
        self._sources = newSources
            
    def update(self, old_selector, new_selector, source=None, depth=0):
        # (None, None) signals that the data is out of date, 
        #  but there is nothing for dependents to do yet.
        self._awaiting_apply = True
        super(Composable, self).update(old_selector, new_selector, source, depth)
          
    def apply(self):
        for source in self.sources:
            if isinstance(source, Composable):
                source.apply()
        if self._awaiting_apply:
            self._apply()
        self._debounce_scanners()
        
    def _apply(self):
        raise NotImplementedError("The base composable class' apply() must be overridden.")
