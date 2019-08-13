class Retriever(object):
    __slots__ = ('source', 'selector', 'getter')
    def __init__(self, source, getter, selector=None):
        self.source = source
        self.selector = selector
        self.getter = getter
    
    def _resolveGenerator(self):
    	"""Slices are calculated here.
		Creating the generator here simplifies and allows us to degenerate
		  the base case and bypass complexity, if possible.
    	"""
        if self.selector is None:
            return (v for v in self.source)
        elif isinstance(self.selector, slice):
            return (v for v in islice(iterable, selector.start, selector.stop, selector.step))
        else:
            return (v for v in self.source if self.selector(v))

    def __iter__(self):
        for entry in self._resolveGenerator():
            yield entry 