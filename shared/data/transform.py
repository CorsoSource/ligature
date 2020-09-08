from shared.data.compose import Composable
from shared.data.scanner import Scanner

class Transform(Composable):
    """This generally regroups or structures data, but does not 
       calculate new data.
    """    
    ScanClass = Scanner
    
    def _graph_attributes(self):
        label = '%s\\lOut: %s' % (
            type(self).__name__,
            ', '.join(self._resultSet._RecordType._fields))
        return {
            'label': label,
            'shape': 'doubleoctagon'
        }
    
    def _apply(self):
        self.transform()
        self._needsUpdate = False
