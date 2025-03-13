from __future__ import annotations

from typing import Sequence, Any, Tuple

from mql.base.bson import BSONType, BSONValue, BSONArray, BSONElement, BSONDocument

from fpy.control.monad import do
from fpy.parsec.parsec import parser, one, ptrans, many, toSeq, neg
from fpy.composable.collections import trans0
from fpy.data.function import const
from fpy.data.either import Either, Right, Left
from fpy.utils.placeholder import __
from fpy.debug.debug import trace

EOO = one(__ == 0)

def takeNBytes(n) -> parser[int, Sequence[int]]:
    return toSeq(one(const(True))) * n

def bytesToInt(b: Sequence[int]) -> int:
    print(f"bytes to int: {b = }")
    res = int.from_bytes(bytearray(b), "little")
    print(f"{res = }")
    return res

def nBytesToInt(n) -> parser[int, int]:
    return ptrans(takeNBytes(n), trans0(bytesToInt))

parseCStr = ptrans(many(toSeq(one(__ != 0))) << toSeq(one(__ == 0)), trans0(lambda lst: ''.join(map(lambda c: chr(c), lst))))

@parser
@do
def parseDocument(b: Sequence[int]) -> Either[Any, Tuple[BSONDocument, Sequence[int]]]:
    with (nBytesToInt(4)(b) as (docSize, rest),
          takeNBytes(docSize - 4)(rest) as (docBytes, rest),
          (many(toSeq(parseElement)) << EOO)(docBytes) as (elms, rest)):
        print(f'{docSize = }')
        return Right((BSONDocument(elms), rest))

@parser
@do
def parseElement(b: Sequence[int]) -> Either[Any, Tuple[BSONElement, Sequence[int]]]:
    with (one(const(True))(b) as (tag, rest),
          parseCStr(rest) as (fieldName, payload)):
        if tag not in  iter(BSONType):
            return Left(f"Unknown BSON tag: {tag}")
        with parseValue(tag, payload) as (val, rest):
            return Right((BSONElement(fieldName, val), rest))
        

@do
def parseValue(tag: BSONType, payload: Sequence[int]) -> Either[Any, Tuple[BSONValue, Sequence[int]]]:
    return Left("Not done yet")

if __name__ == "__main__":
    bson_bytes = [
            12,0,0,0,
            1, 65, 0, 1, 0, 0, 0,
            0
            ]
    print(f"{bson_bytes}")
    print(parseDocument(bson_bytes))
