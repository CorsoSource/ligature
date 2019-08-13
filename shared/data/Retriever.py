class Retriever(object):
    __slots__ = ('source', 'getter', 'selector')
    def __init__(self, source, getter, selector=None):
        self.source = source
        self.selector = selector
        self.getter = getter
    
    def _resolveGenerator(self):
        if self.selector is None:
            return (self.getter(v) for v in self.source)
        elif isinstance(self.selector, slice):
            return (self.getter(v) for v in islice(iterable, selector.start, selector.stop, selector.step))
        else:
            return (self.getter(v) for v in self.source if self.selector(v))

    def __iter__(self):
        for entry in self._resolveGenerator():
            yield entry 