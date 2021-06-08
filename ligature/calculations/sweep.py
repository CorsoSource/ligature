from ligature.calculation import Calculation
from ligature.scanners.element import ElementScanner


class Sweep(Calculation):
    
    ScanClass = ElementScanner
   
    # Record by record
    def calculate(self):
        """Run the function by row creating a new group.
           If groups don't matter, this is easiest.
           Each resulting group is an update.
        f(a,b)=a+b 
        rs.a = [(1,2,3,4),(5,6),(7,8,9)]
        rs.b = [(0,1,0,1),(0,1),(0,1,0)]
        calc = [(1,3,3,5,5,7,7,9,9)]     # 1 group of 9
        """
        self._resultset.append(self.function(*values)
                               for values 
                               in zip(*self.scanners))