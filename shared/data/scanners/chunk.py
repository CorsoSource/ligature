from shared.data.scanner import Scanner


class ChunkScanner(Scanner):
    """For a field in a source, this emits values in chunks
       - one chunk for each group in the source.
    """
    def __iter__(self):
        self._pending_finally()
        for group in self.source._groups[self._group_cursor:]: # error here merely stops iteration
            self._group_cursor += 1
            yield tuple(self.getter(record) for record in group)
            
    def rewind(self, steps=1):
        """Go back the given number of steps in the iteration."""
        while steps > 0:
            steps -= 1
            # If we have a nonzero group cursor, then just decrement it
            if self._group_cursor > 0:
                self._group_cursor -= 1
            else: # already at the start
                return
