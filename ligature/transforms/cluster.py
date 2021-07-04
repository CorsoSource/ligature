from ligature.transform import Transform
from ligature.recordset import RecordSet
from ligature.scanners import RecordScanner


class Cluster(Transform):
    """Group records by a key_field value. Useful for bunching data together for aggregation.
    """

    __slots__ = ('_key_field',)

    ScanClass = RecordScanner

    def __init__(self, source, key_field, *args, **kwargs):
        # Initialize mixins
        super(Cluster, self).__init__(*args, **kwargs)
        
        self._key_field = key_field
        self.sources = (source,)
        self._resultset = RecordSet(recordType=source._RecordType)
        self.scanners = (self.ScanClass(source),)
        
    def transform(self):
        
        last_key_value = None

        groups = []
        group = []
        
        if self._resultset._groups:
            last_key_value = self._resultset._groups[-1][-1][self._key_field]
            
            # loop one: fill for continuity
            for entry in self.scanners[0]:
                if entry[self._key_field] == last_key_value:
                    self._resultset._groups[-1] += (entry,)
                else:
                    group = [entry]
                    last_key_value = entry[self._key_field]
                    break
                
        for entry in self.scanners[0]:
            if entry[self._key_field] == last_key_value:
                group.append(entry)
            else:
                groups.append(group)
                group = [entry]
                last_key_value = entry[self._key_field]
        else:
            groups.append(group)
            
        self._resultset.extend(groups)

