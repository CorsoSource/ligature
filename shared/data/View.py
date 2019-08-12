from itertools import izip as zip
from itertools import islice


def iterSlice(iterable, selector):
    return islice(iterable, selector.start, selector.stop, selector.step)


class View(object):
    """Scan and iterate over data, pointing to the canonical source.
        
    TODO:
     - Reimplement iterSlice to track where it left off
       - Use source info to determine if more data available for generator
     - Implement iterSlice to return windows instead of steps
     - Use caching to make slices of slices work 
       (though iterSlice should step ok, that's not likely to be desired in practice)
    """
    
    __slots__ = ('_sources', '_columns', '_slices', '_getters')
    # sources is a tuple of RecordSet or View references
    # columns are the names to resolve against the RecordSets (in case they overlap)
    # slices is a tuple of lists of slices that get applied against the RecordSets
    
    # EDIT: NO, make a new View from scratch...
    #    !x - If the sources changes, the getters need to be recalculated
    
    # If a source updates, the slices need to be recalculated
    #   - note that the slices may only partially need to be recalculated
    
    def __init__(self, sources, columns, selector=slice(None,None,None)):
        self._columns = columns
        self._resolve(sources)
        
        self._slices = tuple(selector for i in range(len(columns)))
    
    def _resolve(self, sources):
        """Sources is treated as a stack: last in, first out. 
        Thus if we have sources = (s1,s2), and both have column 'z',
          then we expect to resolve to s2's z
        """
        rootSources = []
        getters = []
        for column in self._columns:
            for source in reversed(sources):
                if isinstance(source, View):
                    raise NotImplementedError("Views on views is not functional yet. See TODO list.")
                    try:
                        columnIndex = source._columns.index(column)
                        getters.append(source._getters(columnIndex))
                        rootSources.append(source._sources[columnIndex])
                        break
                    except ValueError: # not found
                        pass
                else:
                    if column in source._RecordType._lookup:
                        ix = source._RecordType._lookup[column]
                        getters.append( lambda record,ix=ix: record._tuple[ix] )
                        rootSources.append(source._records)
                        break
            else: # should not complete loop - a column must be found and break!
                raise ValueError('Column "%s" not found in sources!') #' \n\t%s' % (column, '\n\t'.join('{%s}' % s._lookup.keys() for s in reversed(self.sources)))

        self._sources = tuple(rootSources)
        self._getters = tuple(getters)
    
    @staticmethod
    def _createGenerator(source,selector,getter):
        return ( getter(record) for record in iterSlice(source, selector) )
    
    def _generators(self):
        sourceGenerators =[]
        for columnSourcing in zip(self._sources, self._slices, self._getters):
            sourceGenerators.append( self._createGenerator(*columnSourcing) )
        return sourceGenerators
            
    def __iter__(self):
        return zip(*self._generators())
    
    
    def aggregate(self,):
        """Iterate by windows"""
        pass
    
    def sweep(self,):
        """Iterate row by row over the source data"""
        pass