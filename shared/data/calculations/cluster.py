from shared.data.calculation import Calculation
from shared.data.scanners.chunk import ChunkScanner


class Cluster(Calculation):
    
    ScanClass = ChunkScanner
    
    # By group's records
    def calculate(self):
        """For each group, run the function by row, keeping grouping.
           Use this to maintain groupings for later aggregates.
        f(a,b)=a+b 
        rs.a = [(1,2,3,4),(5,6),(7,8,9)]
        rs.b = [(0,1,0,1),(0,1),(0,1,0)]
        calc = [(1,3,3,5),(5,7),(7,9,9)] # 3 groups
        """
        self._resultSet.extend(
            [ tuple( self.function(*rowValues)
                     for rowValues 
                     in zip(*groupedValues) )
             for groupedValues 
             in zip(*self.scanners)
            ])