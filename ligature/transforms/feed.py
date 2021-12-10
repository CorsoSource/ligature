from ligature.recordset import RecordSet
from ligature.transform import Transform
from ligature.compose import Composable
from ligature.scanners.record import RecordScanner



class Feed(Transform):
    
    __slots__ = ('_key_fields', '_source_keys')

    ScanClass = RecordScanner
    
    def __init__(self, sources, renamed_fields, key_fields=tuple(), *args, **kwargs):
        super(Feed, self).__init__(*args, **kwargs)
    
        self._resultset = RecordSet(recordType=tuple(renamed_fields) + tuple(key_fields))
        
        self.sources = tuple()
        self.scanners = tuple()
        self._key_fields = key_fields
        self._source_keys = tuple()
        
        if sources:
            for source in sources:
                self.add_source(source)
                
    
    def add_source(self, source, key=None):
        if isinstance(source, Composable):
            source = source.results
        self._add_source(source)
        self.scanners += (self.ScanClass(source),)
        if not key:
            self._source_keys += (tuple(
                    lambda record, k=key: record[key]
                    for key in self._key_fields
                ),)
        else:
            self._source_keys += (tuple(
                    lambda record, k=key_value: k
                    for key_value in key
                ),)

    def del_source(self, source):
        if isinstance(source, Composable):
            source = source.results
        self._del_source(source)
        ix_to_remove = set()
        for ix, (scanner, key_functions) in enumerate(zip(self.scanners, self._source_keys)):
            if scanner.source is source:
                ix_to_remove.append(ix)
        
        self.scanners     = tuple(s for ix, s in enumerate(self.scanners)     if not ix in ix_to_remove)
        self._source_keys = tuple(k for ix, k in enumerate(self._source_keys) if not ix in ix_to_remove)
        
        
    def transform(self):
                
        self._resultset.extend(
            # use a generator to avoid adding empty entries
            ( tuple(record) + tuple(getter(record) for getter in source_keys)
              for record in scanner)
            for scanner, source_keys 
            in zip(self.scanners, self._source_keys)
        )