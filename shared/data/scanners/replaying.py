from ..scanner import Scanner 
from .element import ElementScanner
from .chunk import ChunkScanner
from .record import RecordScanner
from .group import GroupScanner


class ReplayingScanner(Scanner):
    """Continues to yield from the same place until ready is called."""
    
    __slots__ = ('_group_anchor', '_record_anchor',)
    
    def __init__(self, *args, **kwargs):
        super(ReplayingScanner, self).__init__(*args, **kwargs)
    
    def __iter__(self):
        try:
            self._iterating_group = True
            for value in super(ReplayingScanner, self).__iter__():
                yield value 
        finally:
            self._iterating_group = False
    
    def _iterGroup(self, group):
        try:
            self._iterating_record = True
            for record in super(ReplayingScanner, self)._iterGroup(group):
                yield record
        finally:
            self._iterating_record = False

    # Replaying scanner already is a state machine compensating for iteration weirdness.
    # But should be refactored to match, regardless.
    def _pending_finally(self):
        pass
    def _iterGroup_finally(self):
        pass
    def _iterRecord_finally(self):
        pass    

    def reset(self):
        super(ReplayingScanner, self).reset()
        self.anchor()
        
    def anchor(self):
        """Moves the last acknowledged position to the current."""
        # Cleanup after iteration means this function can see
        #   two types of values: one while iterating and one statically.
        if self._iterating_group and self._iterating_record:
            self._group_anchor = self._group_cursor - 1
        else:
            self._group_anchor = self._group_cursor
        
        self._record_anchor = self._record_cursor


class ReplayingElementScanner(ReplayingScanner, ElementScanner):
    """Emits a record's field data until exhausted like a normal generator,
       but if the ratchet isn't set, the next call to the generator
       will simply resume from where the last ratchet was.
    """
    def __iter__(self):
        self._record_cursor = self._record_anchor
        self._group_cursor = self._group_anchor
            
        return super(ReplayingElementScanner,self).__iter__()

    
class ReplayingChunkScanner(ReplayingScanner, ChunkScanner):
    """Emits groups until exhausted like a normal generator,
       but if the ratchet isn't set, the next call will 
       repeat from where the last ratchet was.
    """
    def __iter__(self):
        self._group_cursor = self._group_anchor
        return super(ReplayingChunkScanner,self).__iter__()


class ReplayingRecordScanner(ReplayingScanner, RecordScanner):
    """Emits a record's field data until exhausted like a normal generator,
       but if the ratchet isn't set, the next call to the generator
       will simply resume from where the last ratchet was.
    """
    def __iter__(self):
        self._record_cursor = self._record_anchor
        self._group_cursor = self._group_anchor
            
        return super(ReplayingRecordScanner,self).__iter__()


class ReplayingGroupScanner(ReplayingScanner, GroupScanner):
    """Emits groups until exhausted like a normal generator,
       but if the ratchet isn't set, the next call will 
       repeat from where the last ratchet was.
    """
    def __iter__(self):
        self._group_cursor = self._group_anchor
        return super(ReplayingGroupScanner,self).__iter__()        