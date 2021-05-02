from ligature.calculation import Calculation
from ligature.scanners.element import ElementScanner


class Aggregate(Calculation):
    
    ScanClass = ElementScanner
    
    def calculate(self):
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
        
        self._resultSet.clear()
        
        self._resultSet.append( (self.function(*self.scanners),) )