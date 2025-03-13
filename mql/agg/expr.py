from __future__ import annotations

from abc import ABC, abstractmethod

from dataclasses import dataclass
from typing import NewType, Mapping, List, Callable
from enum import Enum

from mql.base.bson import BSONDocument, BSONArray, BSONValue, BSONType
from mql.base.path import Path

from fpy.data.either import Either, Right, Left

VarEnv = NewType("VarEnv", Mapping[str, BSONValue])

class AggExpr(ABC):
    @abstractmethod
    def evaluate(self, doc: BSONDocument, variables: VarEnv) -> Either[str, BSONValue]:
        raise NotImplementedError

@dataclass
class ConstExpr(AggExpr):
    value: BSONValue

    def evaluate(self, doc, variables):
        return Right(self.value)


class AggOperator(Enum):
    pass

AggOperatorLogic: Mapping[AggOperator, Callable[[List[AggExpr], BSONDocument, VarEnv], Either[str, BSONValue]]] = dict()

@dataclass
class OpExpr(AggExpr):
    op: AggOperator
    args: List[AggExpr]

    def evaluate(self, doc, variables):
        evaluator = AggOperatorLogic.get(self.op, None)
        if evaluator is None:
            return Left(f"Operator {self.op} is not defined")
        return evaluator(self.args, doc, variables)

@dataclass
class FieldPathExpr(AggExpr):
    path: Path

    def evaluate(self, doc, variables):
        return Left("Not yet implemented")
