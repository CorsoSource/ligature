from shared.data.RecordSet import RecordSet
from shared.data.RecordType import genRecordType
from shared.data.Retriever import Retriever


def getArguments(function):
    return function.__code__.co_varnames[:function.__code__.co_argcount]


class Calc(object):
    """Base class for sweeping over RecordSets.
    """
    __slots__ = ('sources', 'results', 'function', 
                 '_retrievers','_mapInputs', '_calculated')
    
    def __init__(self, sources, function, labelOutput=[], mapInputs={}):
        self._calculated = False
        self.sources = sources
        self.results = RecordSet(genRecordType(labelOutput))
        self.function = function
        self._mapInputs = mapInputs
        self._resolveSources()

    def _resolveSources(self):
        """Sources is treated as a stack: last in, first out. 
        Thus if we have sources = (s1,s2), and both have column 'z',
          then we expect to resolve to s2's z
        """
        retrievers = []
        columns = [self._mapInputs.get(arg,arg) for arg in getArguments(self.function)]
        for column in columns:
            for source in reversed(self.sources):
                if column in source._RecordType._lookup:
                    ix = source._RecordType._lookup[column]
                    getter = lambda record,ix=ix: record._tuple[ix]
                                        
                    retrievers.append(Retriever(
                        source._records,
                        getter
                    ))
                    break
            else: # should not complete loop - a column must be found and break!
                raise ValueError('Column "%s" not found in sources!' % column) #' \n\t%s' % (column, '\n\t'.join('{%s}' % s._lookup.keys() for s in reversed(self.sources)))

        self._retrievers = tuple(retrievers)

    def _calculate(self):
        generators = tuple(retriever._resolveGenerator()
                           for retriever
                           in self._retrievers)
        self.results._records = [self.results._RecordType(self.function(*values))
                                 for values 
                                 in zip(*generators)]
        self._calculated = True # False
    
    
    def precalc(function):
        @functools.wraps(function)
        def ensureCalculated(self, *args, **kwargs):
            if not self._calculated:
                self._calculate()
            return function(self, *args, **kwargs)
        return ensureCalculated
    
    @precalc
    def __iter__(self):
        return (r for r in self.results)

    @precalc
    def __getitem__(self, selector):
        
        if isinstance(selector, (int,slice)):
            return self.results[selector]
        elif isinstance(selector, int):
            return getattr(self, self._RecordType._fields[ix])
        elif isinstnace(selector, (str,unicode)):
            ix = self.results._RecordType._lookup[selector]
            return getattr(self.results, self.results._RecordType._fields[ix])
            