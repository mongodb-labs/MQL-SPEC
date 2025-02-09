"""
This is an abstraction of BSON, not it's actual serialized format
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, List
from fpy.data.maybe import Maybe, Just, Nothing


class BSONType(Enum):
    MinKey = -1
    EOO = 0
    Number = 1
    String = 2
    Document = 3
    Array = 4
    Binary = 5
    Undefined = 6
    ObjectId = 7
    Boolean = 8
    Datetime = 9
    Null = 10
    Regex = 11
    DBRef = 12
    Code = 13
    Symbol = 14
    CodeWS = 15
    Int32 = 16
    Timestamp = 17
    Int64 = 18
    Decimal128 = 19
    MaxKey = 127


@dataclass
class BSONElement:
    bsonType: BSONType
    fieldName: str
    value: Any

    def doc(self) -> Maybe[BSONDocument]:
        if self.bsonType == BSONType.Document:
            return Just(self.value)
        if self.bsonType == BSONType.Array:
            return Just(BSONDocument(self.value.elements))
        return Nothing()

    def __repr__(self):
        if self.bsonType == BSONType.EOO:
            return "#<EOO>"
        return f"#<{self.bsonType} {self.fieldName}: {self.value.__repr__()}>"

    @classmethod
    def eoo(cls):
        return cls(BSONType.EOO, "", None)

    @classmethod
    def fromValue(cls, val, fieldName: str, typ: BSONType | None = None):
        if isinstance(val, BSONElement):
            return cls(val.bsonType, fieldName, val.value)
        if typ is not None:
            return cls(typ, fieldName, val)
        if isinstance(val, dict):
            return cls(BSONType.Document, fieldName, BSONDocument.fromDict(val))
        if isinstance(val, list):
            return cls(BSONType.Array, fieldName, BSONArray.fromList(val))
        if isinstance(val, int):
            return cls(BSONType.Int32, fieldName, val)
        if isinstance(val, float):
            return cls(BSONType.Number, fieldName, val)
        if isinstance(val, bool):
            return cls(BSONType.Boolean, fieldName, val)
        if isinstance(val, str):
            return cls(BSONType.String, fieldName, val)
        return BSONElement.eoo()

    @staticmethod
    def compare(a, b) -> Maybe[int]:
        if a.bsonType in [BSONType.Number, BSONType.Int32, BSONType.Int64] and b.bsonType in [BSONType.Number, BSONType.Int32, BSONType.Int64]:
            return Just(0 if a.value == b.value else (-1 if a.value < b.value else 1))
        return Nothing()


@dataclass
class BSONDocument:
    elements: List[BSONElement]

    def __contains__(self, fieldName: str) -> bool:
        for elem in self.elements:
            if elem.fieldName == fieldName:
                return True
        return False

    def __getitem__(self, fieldName: str) -> Maybe[BSONElement]:
        for elem in self.elements:
            if elem.fieldName == fieldName:
                return Just(elem)
        return Nothing()

    def __repr__(self):
        return self.elements.__repr__()

    def __len__(self):
        return len(self.elements)

    @classmethod
    def fromDict(cls, dic: dict):
        elms = []
        for k, v in dic.items():
            elms.append(BSONElement.fromValue(v, k))
        return cls(elms)

@dataclass
class BSONArray:
    elements: List[BSONElement]

    def __contains__(self, idx: int) -> bool:
        return idx >= 0 and idx < len(self.elements)

    def __getitem__(self, idx: int) -> Maybe[BSONElement]:
        if idx in self:
            return Just(self.elements[idx])
        return Nothing()

    def __repr__(self):
        return self.elements.__repr__()

    def __len__(self):
        return len(self.elements)

    @classmethod
    def fromList(cls, lst):
        elms = []
        for idx, elm in enumerate(lst):
            elms.append(BSONElement.fromValue(elm, f"{idx}"))
        return cls(elms)

