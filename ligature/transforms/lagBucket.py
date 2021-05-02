from ligature.transform import Transform
from ligature.recordset import RecordSet
from ligature.scanners.record import RecordScanner

from itertools import izip as zip


class LagBucket(Transform):
    """Creates a new recordset of tuples of each of the given records,
    with the first being from `lag` records back.
    """
    __slots__ = ('_lag', '_lagRecords')
    ScanClass = RecordScanner
    
    def __init__(self, source, lag=1):
        #Initialize mixins
        super(LagBucket, self).__init__()
        
        self._lag = lag
        self.sources = (source,)
        self._resultSet = RecordSet(recordType=source._RecordType)
        self.scanners = (self.ScanClass(source),)
        self._lagRecords = []
        
    def transform(self):
        while len(self._lagRecords) < self._lag:
            self._lagRecords.append(next(self.scanners[0]))
        else:
            for record in self.scanners[0]:
                prev = self._lagRecords.pop(0)
                self._resultSet.append(
                    # cast to the record early so the tuples are not misunderstood
                        tuple(
                            (last,this)
                            for last,this
                            in zip(prev._tuple,record._tuple) ) )
                self._lagRecords.append(record)