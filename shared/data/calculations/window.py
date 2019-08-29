from shared.data.calculation import Calculation
from ..scanners.chunk import ChunkScanner
from ..calculations.sweep import Sweep


class Window(Sweep, Calculation):

    ScanClass = ChunkScanner
    
    def calculate(self):
        """Run the aggregate function by group creating one new group.
           If groups don't matter after windowing, this is easiest.
           Each resulting group is an update.
        f(a,b)=sum(a)-sum(b) 
        rs.a = [(1,2,3,4),(5,6),(7,8,9)]
        rs.b = [(0,1,0,1),(0,1),(0,1,0)]
        calc = [(8,10,23)]               # 1 group of 3
        """
        super(Window, self).calculate()