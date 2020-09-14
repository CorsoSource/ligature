from shared.data.scanners.chunk import ChunkScanner


class GroupScanner(ChunkScanner):
    """Returns the whole group when emitting."""
    def __iter__(self):
		self._pending_finally()
        for group in self.source._groups[self._group_cursor:]:
            self._group_cursor += 1
            yield group
