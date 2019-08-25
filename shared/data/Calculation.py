from shared.data.scanners import Scanner
from shared.data.update import UpdateModel


def getArguments(function):
    return function.__code__.co_varnames[:function.__code__.co_argcount]


class Calculation(UpdateModel):
    """Base class for sweeping over RecordSets.
    """
    __slots__ = ('function', 'scanners', 
                 '_mapInputs', '_resultSet', '_calculated')
    
    ScanClass = Scanner
    
    def __init__(self, sources, function, outputLabels, mapInputs={}):
        # Initialize mixins
        super(Calculation, self).__init__(sources, function, outputLabels, mapInputs={})
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
                    scanners.append(self.ScanClass(source, column))
                    source.subscribe(self)
                    break
                    
            else: # should not complete loop - a column must be found and break!
                raise ValueError('Column "%s" not found in sources!' % column) #' \n\t%s' % (column, '\n\t'.join('{%s}' % s._lookup.keys() for s in reversed(self.sources)))

        self.scanners = tuple(scanners)
        
    def calculate(self):
        for source in self.sources:
            if isinstance(source, Calculation):
                source.calculate()
        if not self._calculated:
            self._calculate()
            
    def precalc(function):
        @functools.wraps(function)
        def ensureCalculated(self, *args, **kwargs):
            if not self._calculated:
                self.calculate()
            return function(self, *args, **kwargs)
        return ensureCalculated
    
    @precalc
    def __iter__(self):
        return (group for group in self.results)

    @precalc
    def __getitem__(self, selector):
        return self._resultSet[selector]

    @property
    @precalc
    def results(self):
        return self._resultSet

    def _calculate(self):
        raise NotImplementedError


class Sweep(Calculation):
    
    ScanClass = RowScanner
   
    # Record by record
    def _calculate(self):
        """Run the function by row creating a new group.
           If groups don't matter, this is easiest.
        f(a,b)=a+b 
        rs.a = [(1,2,3,4),(5,6),(7,8,9)]
        rs.b = [(0,1,0,1),(0,1),(0,1,0)]
        calc = [(1,3,3,5,5,7,7,9,9)]     # 1 group of 9
        """
        self._resultSet.append(self.function(*values)
                               for values 
                               in zip(*self.scanners))
        self._calculated = True
        
    def update(self, oldSelector, newSelector):
        # self._calculated = False
        raise NotImplementedError


class Cluster(Calculation):
    
    ScanClass = GroupScanner
    
    # By group's records
    def _calculate(self):
        """For each group, run the function by row, keeping grouping.
           Use this to maintain groupings for later aggregates.
        f(a,b)=a+b 
        rs.a = [(1,2,3,4),(5,6),(7,8,9)]
        rs.b = [(0,1,0,1),(0,1),(0,1,0)]
        calc = [(1,3,3,5),(5,7),(7,9,9)] # 3 groups
        """
        self._resultSet.extend([self.function(*groupedValues)
                                for groupedValues
                                in zip(*self.scanners)])
        self._calculated = True
        
    def update(self, oldSelector, newSelector):
        # self._calculated = False
        raise NotImplementedError


class Window(Calculation):

    ScanClass = ReplayingGroupScanner
    
    def _calulate(self):
        """Run the aggregate function by group creating one new group.
           If groups don't matter after windowing, this is easiest.
        f(a,b)=sum(a)-sum(b) 
        rs.a = [(1,2,3,4),(5,6),(7,8,9)]
        rs.b = [(0,1,0,1),(0,1),(0,1,0)]
        calc = [(8,10,23)]               # 1 group of 3
        """
        raise NotImplementedError
        
    def update(self, oldSelector, newSelector):
        # self._calculated = False
        raise NotImplementedError


class Aggregate(Calculation):
    
    ScanClass = ReplayingGroupScanner
    
    def _calculate(self):
        """Run the aggregate function by group, each creating a new group.
           Useful for aggregates that may be used with another calc's groups.
        f(a,b)=sum(a)-sum(b) 
        rs.a = [(1,2,3,4),(5,6),(7,8,9)]
        rs.b = [(0,1,0,1),(0,1),(0,1,0)]
        calc = [(8,),(10,),(23,)]        # 3 groups of 1
        """
        raise NotImplementedError
        
    def update(self, oldSelector, newSelector):
        # self._calculated = False
        raise NotImplementedError