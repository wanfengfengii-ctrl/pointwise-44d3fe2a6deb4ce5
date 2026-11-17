class SourceCursor:
    def __init__(self, rows):
        self._iterator = iter(rows)
        self.position = 0
        self.exhausted = False

    def __iter__(self):
        return self

    def __next__(self):
        if self.exhausted:
            raise StopIteration
        try:
            row = next(self._iterator)
        except StopIteration:
            self.exhausted = True
            raise
        self.position += 1
        return row
