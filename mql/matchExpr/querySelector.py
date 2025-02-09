from __future__ import annotations


from abc import ABC, abstractmethod
from mql.base.bson import BSONDocument, BSONType, BSONElement,BSONArray
from mql.base.path import Path
from dataclasses import dataclass
from enum import Enum
from typing import List, Any, Callable, Generator, Dict, Optional
from fpy.data.maybe import Maybe, isJust, fromMaybe, Nothing, Just, maybe, isNothing
from fpy.composable.function import func
from fpy.debug.debug import showio
from fpy.data.function import const


class MatchableExpression(ABC):
    """
    An interface for matchable expressions such as PathMatchExpression and TreeExpression
    """
    @abstractmethod
    def matches(self, doc: BSONDocument) -> bool:
        pass
    


class MatchOperator(Enum):
    EQ = "$eq"
    LTE = "$lte"
    LT = "$lt"
    GTE = "$gte"
    GT = "$gt"
    REGEX = "$regex"
    NEAR = "$near"
    NEAR_SPHERE = "$nearSphere"
    GEO_NEAR = "$geoNear"

OperatorArity: Dict[MatchOperator, int] = dict()
OperatorKW: Dict[MatchOperator, List[str]] = dict()
OperatorLogic: Dict[MatchOperator, Callable[[BSONElement, BSONElement, Optional[Dict]], bool]] = dict()

def defop(operator, arity, kw):
    global OperatorLogic
    def res(fn):
        OperatorLogic[operator] = fn
        OperatorArity[operator] = arity
        OperatorKW[operator] = kw
        return fn
    return res


@dataclass
class Predicate:
    operator: MatchOperator
    argument: BSONElement
    namedArguments: Optional[Dict] = None

    def eval(self, elem: BSONElement) -> bool:
        return OperatorLogic.get(self.operator, const(False))(elem, self.argument, self.namedArguments)


@dataclass
class PathMatchExpression(MatchableExpression):
    path: Path
    predicate: Predicate

    def matches(self, doc: BSONDocument) -> bool:
        leafElms = PathMatchExpression.iterPath(self.path, BSONElement(BSONType.Document, "", doc))
        for elem in leafElms:
            if self.predicate.eval(elem):
                return True
        return False


    @staticmethod
    def iterPath(path: Path, doc: BSONElement) -> List[BSONElement]:
        
        if not doc:
            return []
        if not path:
            return [doc]
    
        if path and doc.bsonType not in [BSONType.Array, BSONType.Document]:
            return []
    
        while (path and doc.bsonType == BSONType.Document):
            nxt = doc.value[path.head()]
            doc = fromMaybe(BSONElement.eoo(), nxt)
            path = path.tail()

        if doc.bsonType == BSONType.EOO:
            return []
    
        if not path:
            # We are at the end of the path
            if doc.bsonType == BSONType.Array:
                return doc.value.elements
            return [doc]
    
        # We should arrive at an array or scaler here
    
        if doc.bsonType != BSONType.Array:
            return [doc]
    
        return PathMatchExpression.iterArray(path, doc)
    
    @staticmethod
    def iterArray(path: Path, doc: BSONElement) -> List[BSONElement]:
        head = path.head()
        arr: BSONArray = doc.value
    
        if head.isdigit():
            # Array offset match
            def innerDispatch(restPath: Path, elm: BSONElement) -> List[BSONElement]:
                if elm.bsonType == BSONType.EOO:
                    return []
                if not restPath:
                    return [elm]
                if elm.bsonType == BSONType.Document:
                    return PathMatchExpression.iterPath(restPath, elm)
                if elm.bsonType == BSONType.Array:
                    return sum(map(lambda e: PathMatchExpression.iterArray(restPath, e),
                                   elm.value.elements),
                               start = [])
                return []
    
            return innerDispatch(path.tail(), fromMaybe(BSONElement.eoo(), arr[int(head)]))
    
        return sum(map(lambda elm: PathMatchExpression.iterPath(path, elm)
                                    if elm.bsonType == BSONType.Document
                                    else [],
                       arr.elements),
                   start = [])

@defop(MatchOperator.EQ, 1, None)
def eq(elem: BSONElement, arg: BSONElement, _):
    cmpRes = BSONElement.compare(elem, arg)
    return fromMaybe(None, cmpRes) == 0

@defop(MatchOperator.LT, 1, None)
def lt(elem: BSONElement, arg: BSONElement, _):
    cmpRes = BSONElement.compare(elem, arg)
    return fromMaybe(None, cmpRes) < 0

@defop(MatchOperator.GT, 1, None)
def gt(elem: BSONElement, arg: BSONElement, _):
    cmpRes = BSONElement.compare(elem, arg)
    return fromMaybe(None, cmpRes) > 0

@defop(MatchOperator.LTE, 1, None)
def lte(elem: BSONElement, arg: BSONElement, _):
    cmpRes = BSONElement.compare(elem, arg)
    return fromMaybe(None, cmpRes) <= 0

@defop(MatchOperator.GTE, 1, None)
def gte(elem: BSONElement, arg: BSONElement, _):
    cmpRes = BSONElement.compare(elem, arg)
    return fromMaybe(None, cmpRes) >= 0

# Tree Operators

class TreeOperator(Enum):
    AND = "$and"
    OR = "$or"
    NOR = "$nor"

TreeOperatorEval = dict()

def defTreeOp(op):
    global TreeOperatorEval
    def res(fn: Callable[[List, BSONDocument], bool]):
        TreeOperatorEval[op] = fn
        return fn
    return res

@dataclass
class TreeExpression(MatchableExpression):
    operator: TreeOperator
    children: List[MatchableExpression]

    def matches(self, doc: BSONDocument) -> bool:
        return TreeOperatorEval.get(self.operator, const(False))(self.children, doc)

@defTreeOp(TreeOperator.AND)
def treeAnd(children: List, doc: BSONDocument) -> bool:
    for child in children:
        if not child.matches(doc):
            return False
    return True

@defTreeOp(TreeOperator.OR)
def treeOR(children: List, doc: BSONDocument) -> bool:
    for child in children:
        if child.matches(doc):
            return True
    return False

@defTreeOp(TreeOperator.NOR)
def treeNOR(children: List, doc: BSONDocument) -> bool:
    for child in children:
        if child.matches(doc):
            return False
    return True
