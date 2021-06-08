from ligature.transform import Transform
from ligature.recordset import RecordSet
from ligature.scanners.replaying import ReplayingGroupScanner, ReplayingRecordScanner

from itertools import izip as zip


class Regroup(Transform):
    """Take the first recordset's grouping and enforce it on another's.
    NOTE: This assumes this can work! No validation (yet) is done.
      Rather, this is a way to flag that two recordsets are aligned.
    """
    ScanClass = (ReplayingGroupScanner, ReplayingRecordScanner)
    
    def __init__(self, source, target):
        # Initialize mixins
        super(Regroup, self).__init__()
        
        self.sources = (source, target)
        self._resultset = RecordSet(recordType=target._RecordType)
        self._generateScanners()
    
    def _generateScanners(self):
        #source, target = self.sources
        #self.scanners = (GroupScanner(source), RecordScanner(target))
        self.scanners = tuple(sc(s) for sc,s in zip(self.ScanClass, self.sources))
        
    def transform(self):
        source, target = self.scanners
        
        for group in source:
            newGroup = tuple(record for _,record in zip(group, target))
            if len(newGroup) == len(group):
                self._resultset.extend( (newGroup,) )
                source.anchor()
                target.anchor()
            else:
                break
