from __future__ import annotations

from typing import Sequence, Any, Tuple, Dict
import struct

from mql.base.bson import BSONType, BSONValue, BSONArray, BSONElement, BSONDocument

from fpy.control.monad import do
from fpy.parsec.parsec import parser, one, ptrans, many, toSeq, neg
from fpy.composable.collections import trans0
from fpy.data.function import const, uncurryN
from fpy.data.either import Either, Right, Left
from fpy.utils.placeholder import __
from fpy.debug.debug import trace

TAG_PARSER: Dict[BSONType, parser[int, BSONType]] = dict()

def defTag(tag: BSONType):
    def res(fn):
        global TAG_PARSER
        p = ptrans(parser(fn), trans0(lambda v: BSONValue(tag, v)))
        TAG_PARSER[tag] = p
        return p
    return res


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

@do
def takePrefixSizedBytes(payload, prefixSize = 4, sizeInclPrefix = False):
    with nBytesToInt(prefixSize)(payload) as (size, rest): 
        return takeNBytes(size - prefixSize if sizeInclPrefix else size)(rest)

parseCStr = ptrans(many(toSeq(one(__ != 0))) << toSeq(one(__ == 0)), trans0(lambda lst: ''.join(map(lambda c: chr(c), lst))))

@parser
@do
def parseDocument(b: Sequence[int]) -> Either[Any, Tuple[BSONDocument, Sequence[int]]]:
    with (takePrefixSizedBytes(b, sizeInclPrefix=True) as (docBytes, rest),
          (many(toSeq(parseElement)) << EOO)(docBytes) as (elms, rest)):
        return Right((BSONDocument(elms), rest))

@parser
@do
def parseElement(b: Sequence[int]) -> Either[Any, Tuple[BSONElement, Sequence[int]]]:
    with (one(const(True))(b) as (tag, rest),
          parseCStr(rest) as (fieldName, payload),
          TAG_PARSER.get(tag, const(Left(f"Undefined Tag: {tag}")))(payload) as (val, rest)):
            return Right((BSONElement(fieldName, val), rest))
        
@defTag(BSONType.Number)
@do
def parseNumber(payload):
    with (takeNBytes(8)(payload) as (b, rest)):
        return Right((struct.unpack("d", bytes(b))[0], rest))
        
@defTag(BSONType.Int32)
@do
def parseI32(payload):
    with (takeNBytes(4)(payload) as (b, rest)):
        return Right((struct.unpack("<i", bytes(b))[0], rest))
        
@defTag(BSONType.Int64)
@do
def parseI64(payload):
    with (takeNBytes(8)(payload) as (b, rest)):
        return Right((struct.unpack("<q", bytes(b))[0], rest))


@defTag(BSONType.String)
@do
def parseStr(payload):
    with takePrefixSizedBytes(payload) as (b, rest):
        return Right((struct.unpack("s", bytes(b))[0], rest))



if __name__ == "__main__":
    bson_bytes = [
            12,0,0,0,
            16, 65, 0, 1, 0, 0, 0, 
            0
            ]
    print(f"{bson_bytes}")
    print(parseDocument(bson_bytes))
