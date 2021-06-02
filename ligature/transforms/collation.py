from ligature.recordset import RecordSet
from ligature.scanners.record import RecordScanner
from ligature.transform import Transform, Composable


from heapq import heapify, heappush, heappop


class Collation(Transform):
    """combine the source recordsets into one recordset.
    The new record type will have all the target fields
      along with the key (and optionally a coalescing) fields.
      
    Each
    """
    
    __slots__ = ('_key_field', '_collation_field', 
                 '_target_fields', '_scanner_coverage',)
    
    ScanClass = RecordScanner
    
    def __init__(self, sources, key_field, target_fields=None, collation_field=None):
        super(Collation, self).__init__()
        self.sources = tuple(sources)
        
        self._key_field = key_field
        self._collation_field = collation_field
        self._target_fields = target_fields
        
        self._resolveSources()
                
    def _resolveSources(self):
        
        rawSources = [source.results if isinstance(source, Composable) else source
                      for source 
                      in self.sources]
            
        scanner_coverage = {}
        scanners = []
        preset_targets = set(self._target_fields or [])
        covered_fields = set()
        
        # Gather all the fields
        for source in rawSources:
            covered_fields = set()
            for field in source._RecordType._fields:
                if field in (self._key_field, self._collation_field,):
                    continue
                    
                if not preset_targets or field in preset_targets:
                    covered_fields.add(field)
            
            # skip nops
            if not covered_fields:
                continue
        
            scanner = RecordScanner(source)
            scanner_coverage[scanner] = covered_fields
            scanners.append(scanner)
        
        all_covered_fields = set()
        for covered_fields in scanner_coverage.values():
            all_covered_fields.update(covered_fields)

        if preset_targets:
            assert preset_targets==all_covered_fields, 'Sources do not cover the target fields: given: %r -- covered: %r' % (preset_targets, covered_fields)
            target_fields = tuple(self._target_fields)
        else:
            target_fields = tuple(field for field in all_covered_fields)

        self._target_fields = target_fields
        
        self._scanner_coverage = scanner_coverage
        
        self.scanners = tuple(scanners)
        self._resultSet = RecordSet(recordType=((self._key_field,) + self._target_fields + ((self._collation_field,) or tuple())))
    
    
    def transform(self):
        
        scanners = set(self.scanners)
        
        def get_next(scanner, remaining=scanners):
            try:
                entry = next(scanner)
                return entry
            except StopIteration:
                remaining.remove(scanner)
                return None

        # initial conditions
        if self._resultSet:
            cursor_values = dict((field, value) for field, value in zip(self._resultSet._RecordType._fields, self._resultSet._groups[-1][-1]))
        else:
            cursor_values = dict((field, None) for field in self._resultSet._RecordType._fields) # initialize to None to ensure _some_ value for all non-key/group fields
        
        cursor_value_heap = []
        for scanner in frozenset(scanners):
            entry = get_next(scanner)
            if entry is not None:
                cursor_value_heap.append((entry[self._key_field], entry, scanner)) # include scanner for replacement lookup later
        heapify(cursor_value_heap)
        
        # generate results
        merged = []

        while cursor_value_heap:

            key_value, entry, scanner = heappop(cursor_value_heap)
                        
            for field in self._scanner_coverage[scanner]:
                cursor_values[field] = entry[field]
            
            cursor_values[self._key_field] = entry[self._key_field]

            if self._collation_field:
                group_value = entry[self._collation_field]

                # when grouping for merge, assume group final value is most recent by key sort value
                if cursor_values[self._collation_field] == group_value and group_value is not None:
                    merged[-1] = self._resultSet._RecordType(cursor_values)
                else:
                    cursor_values[self._collation_field] = group_value
                    merged.append(self._resultSet._RecordType(cursor_values))
            else:
                merged.append(self._resultSet._RecordType(cursor_values))

            entry = get_next(scanner)
            if entry is not None:
                heappush(cursor_value_heap, (entry[self._key_field], entry, scanner))

        if merged:
            self._resultSet.extend([[v for v in merged]])
            
