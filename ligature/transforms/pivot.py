from ligature.transform import Transform
from ligature.recordset import RecordSet
from ligature.scanners.group import GroupScanner

from itertools import izip as zip


class Pivot(Transform):
    """Rotate groups of records into a record of lists.
    [({a:4,b:3},{a:6,b:5},{a:8,b:7}),({a:10,b:9},{a:12,b:11})]
    becomes
    [({a:(4,6,8),b:(3,5,7)}),({a:(10,12),b:(9,11)})]
    """
    ScanClass = GroupScanner

    def __init__(self, source, *args, **kwargs):
        # Initialize mixins
        super(Pivot, self).__init__(*args, **kwargs)

        self.sources = (source,)
        self._resultset = RecordSet(recordType=source._RecordType)
        self.scanners = (self.ScanClass(self.sources[0]),)
        
    def transform(self):
        for group in self.scanners[0]:
            self._resultset.append( 
                # cast to the record early so the tuples are not misunderstood
                self._resultset.coerceRecordType(
                    tuple(zip(*group)) ) )
