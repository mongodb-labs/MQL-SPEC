from dataclasses import dataclass
from abc import ABCMeta
from typing import Iterable, List, Tuple, Sequence, Any
from mql.base.bson import BSONDocument
from enum import Enum

from fpy.parsec.parsec import parser, one, ptrans, many, many1
from fpy.control.monad import do
from fpy.data.maybe import Maybe, Nothing, Just
from fpy.data.either import Either, Left, Right
from fpy.data.function import const
from fpy.composable.collections import trans0

"""
OpMsg Packet:
    uint32_t flags
    DocumentSequence Document[]
    optional<uint32_t> checksum
"""

class OpCode(Enum):
    Invalid = 0
    Insert = 2002
    Query = 2004
    GetMore = 2005
    Msg = 2013

@dataclass
class FlagBits:
    checksumPresent: bool
    moreToCome: bool
    exhaustAllowed: bool

class SectionKind(Enum):
    Document = 0
    DocumentSequence = 1

class Section(ABCMeta):
    kind: SectionKind

@dataclass
class SectionBody(Section):
    document: BSONDocument

@dataclass
class SectionDocumentSequence(Section):
    documentSequenceIdentifier: str
    documents: Iterable[BSONDocument]

@dataclass
class OpMsg:
    messageLength: int
    requestId: int
    responseTo: int
    opCode: OpCode
    flagBits: FlagBits
    sections: Iterable[Section]


def takeNBytes(n) -> parser[int, Sequence[int]]:
    return one(const(True)) * n

def bytesToInt(b: Sequence[int]) -> int:
    return int.from_bytes(bytearray(b), "little")

def nBytesToInt(n) -> parser[int, int]:
    return ptrans(takeNBytes(n), trans0(bytesToInt))

@parser
@do
def parseFlag(msg: Sequence[int]):
    with (nBytesToInt(2)(msg) as (lower, msg),
          nBytesToInt(2)(msg) as (upper, msg)):
        return Right((FlagBits(1 == lower & 1, 1 == (lower >> 1) & 1, 1 == upper & 1), msg))

@parser
@do
def parseBody(b):
    return Left("Not Implemented Yet")

@parser
@do
def parseDocSeq(b):
    return Left("Not Implemented Yet")

@parser
@do
def parseSection(b):
    with nBytesToInt(1)(b) as (kind, rest):
        if kind == 0:
            return parseBody(rest)
        if kind == 1:
            return parseDocSeq(rest)
        return Left(f"Unknown section kind {kind}")

@do
def parseSections(b: Sequence[int]) -> Either[Any, Sequence[Section]]:
    hasBody = False
    work_b = b
    sections: Sequence[Section] = []
    while work_b:
        with parseSection(work_b) as (sec, rest_b):
            if isinstance(sec, SectionBody):
                if hasBody:
                    return Left("Multiple body sections in message")
                hasBody = True
            sections.append(sec)
            work_b = rest_b
    return Right(sections)

@parser
@do
def parseMsg(msg: Sequence[int]):
    with (nBytesToInt(4)(msg) as (msgSize, msg),
          nBytesToInt(4)(msg) as (reqId, msg),
          nBytesToInt(4)(msg) as (resTo, msg),
          nBytesToInt(4)(msg) as (rawOpCode, msg),
          parseFlag(msg) as (flag, msg),
          takeNBytes(msgSize - 16 - 4 - (4 if flag.checksumPresent else 0))(msg) as (sectionBytes, msg),
          takeNBytes(4 if flag.checksumPresent else 0)(msg) as (_, rest),
          parseSections(sectionBytes) as sections):
        for opCode in OpCode:
            if opCode.value == rawOpCode:
                return Right((OpMsg(msgSize, reqId, resTo, opCode, flag, sections), rest))
        return Left(f"Unknown op code: {rawOpCode}")
    


if __name__ == "__main__":
    b = [*bytearray([1,2,3,4])]
    with takeNBytes(4)(b) as (b4, _):
        print(b4)
        print(bytes(b4))

