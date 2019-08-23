from shared.data.Scanners import Scanner


def getArguments(function):
    return function.__code__.co_varnames[:function.__code__.co_argcount]


class Calc(object):
    """Base class for sweeping over RecordSets.
    """
    __slots__ = ('sources', 'function', 'scanners', 
                 '_mapInputs', '_resultSet', '_calculated')
    
    def __init__(self, sources, function, outputLabels, mapInputs={}):
        self._calculated = False
        self._resultSet = RecordSet(recordType=genRecordType(outputLabels))
        self.sources = tuple(sources)
        self.function = function
        self._mapInputs = mapInputs
        self._resolveSources()
    
    def _resolveSources(self):
        """Sources is treated as a stack: last in, first out. 
        Thus if we have sources = (s1,s2), and both have column 'z',
          then we expect to resolve to s2's z
        """
        scanners = []
        columns = [self._mapInputs.get(arg,arg) for arg in getArguments(self.function)]
        for column in columns:
            for source in reversed(self.sources):
                if isinstance(source, Calc):
                    source = source.results
                    
                if column in source._RecordType._lookup:
                    scanners.append(Scanner(source, column))
                    break
                    
            else: # should not complete loop - a column must be found and break!
                raise ValueError('Column "%s" not found in sources!' % column) #' \n\t%s' % (column, '\n\t'.join('{%s}' % s._lookup.keys() for s in reversed(self.sources)))

        self.scanners = tuple(scanners)

    def _calculate(self):
        for source in self.sources:
            if not getattr(source, '_calculated', True):
                source._calculate()

        self._resultSet.append(self.function(*values)
                             for values 
                             in zip(*self.scanners))
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
        return self._resultSet[selector]

    @property
    @precalc
    def results(self):
        return self._resultSet