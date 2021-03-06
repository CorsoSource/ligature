from ligature.scanners.element import ElementScanner


class RecordScanner(ElementScanner):
    """Returns the whole record when emitting."""
    def __iter__(self):
        self._pending_finally()
        for group in self.source._groups[self._group_cursor:]: # error here merely stops iteration
            self._group_cursor += 1
            for record in self._iterGroup(group):
                yield record