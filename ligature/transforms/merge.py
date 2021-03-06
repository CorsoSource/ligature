from ligature.scanners.element import ElementScanner
from ligature.transform import Transform
from ligature.recordset import RecordSet
from ligature.compose import Composable
from ligature.record import genRecordType

from itertools import izip as zip


class Merge(Transform):
    """Combine the source recordsets into one recordset.
    The new record type will have all the source columns,
      with the caveat that later sources win for overlaps.
    """
    ScanClass = ElementScanner
    
    def __init__(self, sources, *args, **kwargs):
        # Initialize mixins
        super(Merge, self).__init__(*args, **kwargs)
        self.sources = tuple(sources)
        self._resolveSources()
        
    
    def _resolveSources(self):
        """Sources may overlap: if so, only take the latter."""        
        rawSources = [source.results if isinstance(source, Composable) else source
                      for source 
                      in self.sources]
        
        allFields = []
        # Gather all the fields
        for source in rawSources:
            for field in source._RecordType._fields:
                allFields.append(field)
        
        scanners = []
        sourceFields = set(allFields)
        for source in reversed(rawSources):
            for field in source._RecordType._fields:
                if field in sourceFields:
                    sourceFields.remove(field)
                    scanners.append( (field, self.ScanClass(source, field)) )
                if not sourceFields:
                    break
            if not sourceFields:
                break
            
        # While we want to prioritize later sources, the fields should
        #   likely keep the same order, starting with the earlier sources.
        # see https://stackoverflow.com/a/12814719/1943640
        scanners.sort(key=lambda entry: allFields.index(entry[0]))
        
        self.scanners = tuple(scanner 
                              for field,scanner 
                              in scanners)
        self._resultset = RecordSet(recordType=genRecordType(field 
                                                             for field,scanner 
                                                             in scanners))
        
    def transform(self):
        """Simply scan down the sources, generating new records."""
        self._resultset.append( tuple(
            self._resultset.coerceRecordType(newRecordValues)
            for newRecordValues
            in zip(*self.scanners)
            ) )        
