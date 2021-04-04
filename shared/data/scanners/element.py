from ..scanner import Scanner


class ElementScanner(Scanner):
    def __iter__(self):
        self._pending_finally()
        for group in self.source._groups[self._group_cursor:]: # error here merely stops iteration
            self._group_cursor += 1
            for record in self._iterGroup(group):
                yield self.getter(record)
        
    def rewind(self, steps=1):
        """Go back the given number of steps in the iteration."""
        while steps > 0:
            steps -= 1
            # If the record cursor is nonzero, back it off by one
            if self._record_cursor > 0:
                # remember, the group cursor aggressively iterates
                if self._group_cursor >= len(self.source._groups):
                    self._group_cursor = len(self.source._groups)-1 
                else:
                    self._record_cursor -= 1
            # If the record cursor is at the start rewind the group
            elif self._group_cursor:
                if self._group_cursor >= len(self.source._groups):
                    self._group_cursor = len(self.source._groups)-1 
                else:
                    self._group_cursor -= 1
                self._record_cursor = len(self.source._groups[self._group_cursor]) -1
            else: # already at the start
                return
