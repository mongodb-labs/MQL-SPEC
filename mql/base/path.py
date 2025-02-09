from typing import List

class Path:
    parts: List[str]

    def __init__(self, parts = None):
        if parts is not None:
            self.parts = parts
        else:
            self.parts = []

    def head(self):
        return self.parts[0]

    def tail(self):
        return Path(self.parts[1:])

    def __bool__(self):
        return bool(self.parts)

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return ".".join(self.parts)

    @classmethod
    def fromString(cls, pathString: str):
        return cls(pathString.split("."))
