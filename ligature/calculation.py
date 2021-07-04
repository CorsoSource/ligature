from ligature.compose import Composable
from ligature.scanner import Scanner
from ligature.recordset import RecordSet
from ligature.record import genRecordType
from ligature.expression import Expression
from ligature.scanners.identity import Identity


def getArguments(function):
    if isinstance(function, Expression):
        return function._fields
    else:
        return function.__code__.co_varnames[:function.__code__.co_argcount]


class Calculation(Composable):
    """Base class for sweeping over RecordSets.
    """
    __slots__ = ('function', '_mapInputs')
    
    ScanClass = Scanner
    
    def __init__(self, sources, function, outputLabels, mapInputs={}, *args, **kwargs):
        # Initialize mixins
        super(Calculation, self).__init__(*args, **kwargs)
      
        self._resultset = RecordSet(recordType=genRecordType(outputLabels))
        self.subscribe(self._resultset)
        self.sources = tuple(sources)

        if isinstance(function, (str,unicode)):
            self.function = Expression(function)
        else:
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
                if column is source:
                    scanners.append(Identity(source))
                    break

                if isinstance(source, (Composable, RecordSet)):
                    if isinstance(source, Composable):
                        source = source.results
                        
                    if column in source._RecordType._lookup:
                        scanners.append(self.ScanClass(source, column))
                        source.subscribe(self)
                        break
                else:
                    try:
                        _ = getattr(source, column)
                        scanners.append(Identity(source, column))
                        break
                    except:
                        pass
                    
            else: # should not complete loop - a column must be found and break!
                raise ValueError('Column "%s" not found in sources!' % column) #' \n\t%s' % (column, '\n\t'.join('{%s}' % s._lookup.keys() for s in reversed(self.sources)))

        self.scanners = tuple(scanners)

    def _apply(self):
        self.calculate()
        self._needsUpdate = False
            
    def _default_graph_attributes(self):
        label = 'In: %s\\lf(x): "%s"\\lOut: %s' % (
            ', '.join(getArguments(self.function)),
            type(self).__name__,
            ', '.join(self._resultset._RecordType._fields))
        return {
            'label': label,
            'shape': 'rect'
        }
    
    def calculate(self):
        raise NotImplementedError("The base calculation class' _calculate() must be overridden.")
