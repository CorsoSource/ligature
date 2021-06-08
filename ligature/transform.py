from ligature.compose import Composable
from ligature.scanner import Scanner

class Transform(Composable):
    """This generally regroups or structures data, but does not 
       calculate new data.
    """    
    ScanClass = Scanner
    
    def _default_graph_attributes(self):
        label = '%s\\lOut: %s' % (
            type(self).__name__,
            ', '.join(self._resultset._RecordType._fields))
        return {
            'label': label,
            'shape': 'doubleoctagon'
        }
    
    def _apply(self):
        self.transform()
        self._needsUpdate = False
