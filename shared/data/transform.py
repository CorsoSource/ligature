from shared.data.compose import Composable


class Transform(Composable):
    """This generally regroups or structures data, but does not 
       calculate new data.
    """
    ScanClass = Scanner
    
    def _apply(self):
        self.transform()
        self._needsUpdate = False


class Pivot(Transform):
    """Rotate groups of records into a record of lists.
    [({a:4,b:3},{a:6,b:5},{a:8,b:7}),({a:10,b:9},{a:12,b:11})]
    becomes
    [({a:(4,6,8),b:(3,5,7)}),({a:(10,12),b:(9,11)})]
    """
    ScanClass = GroupScanner
    
    def __init__(self, source):
        # Initialize mixins
        super(Pivot, self).__init__(source)

        self.sources = (source,)
        self._resultSet = RecordSet(recordType=source._RecordType)
        self._generateScanners()
    
    def _generateScanners(self):
        self.scanners = tuple(self.ScanClass(self.sources[0], field)
                              for field
                              in self.sources[0]._RecordType._fields )            
    
    def transform(self):
        for gs in zip(*self.scanners):
            self._resultSet.append( (gs,) )


class Regroup(Transform):
    """Take one recordset's grouping and enforce it on another's.
    NOTE: This assumes this can work! No validation (yet) is done.
      Rather, this is a way to flag that two recordsets are aligned.
    """
    pass
    #raise NotImplementedError


class Merge(Transform):
    """Combine the source recordsets into one recordset.
    The new record type will have all the source columns,
      with the caveat that later sources win for overlaps.
    """
    pass
    #raise NotImplementedError


class Unvacuum(Transform):
    """Inject empty groups to fill an index.
    Thus, if the index function is by value, and it's currently
    {1,2,3,6,7,9}
    a delta of 1 starting with 1 would add empty groups for
    {4,5,8}
    """
    pass
    #raise NotImplementedError