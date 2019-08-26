from shared.data.scanners import Scanner
from shared.data.compose import Composable


def getArguments(function):
    return function.__code__.co_varnames[:function.__code__.co_argcount]


class Calculation(Composable):
    """Base class for sweeping over RecordSets.
    """
    __slots__ = ('function', 'scanners', '_mapInputs')
    
    ScanClass = Scanner
    
    def __init__(self, sources, function, outputLabels, mapInputs={}):
        # Initialize mixins
        super(Calculation, self).__init__(sources, function, outputLabels, mapInputs={})
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
                if isinstance(source, Composable):
                    source = source.results
                    
                if column in source._RecordType._lookup:
                    scanners.append(self.ScanClass(source, column))
                    source.subscribe(self)
                    break
                    
            else: # should not complete loop - a column must be found and break!
                raise ValueError('Column "%s" not found in sources!' % column) #' \n\t%s' % (column, '\n\t'.join('{%s}' % s._lookup.keys() for s in reversed(self.sources)))

        self.scanners = tuple(scanners)
    
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
    
    def apply(self):
        super(Calculation, self).apply()
        self._debounce_scanners()

    def _apply(self):
        self._calculate()
        self._needsUpdate = False

    def _calculate(self):
        raise NotImplementedError("The base calculation class' _calculate() must be overridden.")


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
        self._resultSet.extend(
            [self._resultSet._RecordType(
                self.function(*rowValues))
             for groupedValues in zip(*self.scanners)
             for rowValues in zip(*groupedValues)
            ])


class Window(Sweep, Calculation):

    ScanClass = GroupScanner
    
    def _calulate(self):
        """Run the aggregate function by group creating one new group.
           If groups don't matter after windowing, this is easiest.
           Each resulting group is an update.
        f(a,b)=sum(a)-sum(b) 
        rs.a = [(1,2,3,4),(5,6),(7,8,9)]
        rs.b = [(0,1,0,1),(0,1),(0,1,0)]
        calc = [(8,10,23)]               # 1 group of 3
        """
        super(Window, self)._calculate()


class Aggregate(Calculation):
    
    ScanClass = RowScanner
    
    def _calculate(self):
        """Run the aggregate function by group, each creating a new group.
           Useful for aggregates that may be used with another calc's groups.
           Collapses down a group to a single value.
        f(a,b)=sum(a)-sum(b) 
        rs.a = [(1,2,3,4),(5,6),(7,8,9)]  = 45
        rs.b = [(0,1,0,1),(0,1),(0,1,0)]  = 4
        calc = [(41,)]                    # 1 group of 1
        """
        for scanner in self.scanners:
            scanner.reset()
        
        self._resultSet._groups = [(
            self._resultSet.coerceRecordType( 
                self.function(*self.scanners)
                )
            ,)]
        g = self._resultSet._groups[0]
        gix = self._resultSet._indexingFunction(g)
        self._resultSet._gindex = {gix:g}