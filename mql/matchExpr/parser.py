from fpy.data.either import Left, Right, isLeft, isRight, fromLeft, fromRight, Either
from fpy.composable.function import func
from fpy.control.functor import fmap
from typing import Any, List, Dict, Callable, Optional
from mql.matchExpr.querySelector import MatchOperator, Predicate, PathMatchExpression, TreeOperator, TreeExpression, MatchableExpression, OperatorArity, OperatorKW, NotExpression
from mql.base.bson import BSONElement, BSONDocument, BSONType
from mql.base.path import Path

PathlessExpressions: Dict[str, Callable[[BSONElement], Either[str, MatchableExpression]]] = dict()

def defPathless(op: TreeOperator):
    global PathlessExpressions
    def res(fn):
        PathlessExpressions[op.value] = fn
        return fn
    return res

MatchOperatorParser: Dict[str, Callable[[str, BSONElement, Optional[BSONDocument]], Either[str, MatchableExpression]]] = dict()

def defMatchOp(op: MatchOperator):
    global MatchOperatorParser
    def res(fn):
        MatchOperatorParser[op.value] = fn
        return fn
    return res

def isDBRefDocument(expr: BSONDocument, allowIncompleteDBRef: bool) -> bool:
    hasRef = "$ref" in expr
    hasId = "$id" in expr
    hasDb = "$db" in expr

    if allowIncompleteDBRef:
        return hasRef or hasId or hasDb

    return hasRef and hasId

def isGeoExpr(expr: BSONDocument) -> bool:
    """
    We are looking for an expression containing one of: $near, $nearSphere, $geoNear
    """
    return "$near" in expr or "$nearSphere" in expr or "$geoNear" in expr


def isExpressionDocument(expr: BSONElement, allowIncompleteDBRef: bool) -> bool:
    if expr.bsonType != BSONType.Document:
        return False

    if not expr.value.elements:
        return False

    if not expr.value.elements[0].fieldName.startswith("$"):
        return False
    
    if isDBRefDocument(expr.value, allowIncompleteDBRef):
        return False

    return True


def parsePredicateTopLevel(expr: BSONDocument) -> Either[str, MatchableExpression]:
    """
QuerySelector := { PredicateTopLevelExpr * }

PredicateTopLevelExpr  := PathlessExpression
                        | DocumentTopLevelExpr
                        | Geo
                        | RegexMatch
                        | Eq
    """
    childrenExpr = []
    for elm in expr.elements:
        if elm.fieldName.startswith("$"):
            matchFn = parsePathlessExpression(elm)
            if isRight(matchFn):
                childrenExpr.append(fromRight(None, matchFn))
                continue
            return matchFn
        if isExpressionDocument(elm, False):
            subExprs = parseDocumentTopLevel(elm.fieldName, elm)
            if isRight(subExprs):
                childrenExpr.extend(fromRight([], subExprs))
                continue
            return subExprs
        if elm.bsonType == BSONType.Regex:
            regexExpr = parseRegexMatch(elm.fieldName, elm)
            if isRight(regexExpr):
                childrenExpr.append(fromRight(None, regexExpr))
                continue
            return regexExpr
        # default case is field equality
        eqExpr = parseComparison(elm.fieldName, elm, MatchOperator.EQ)
        if isRight(eqExpr):
            childrenExpr.append(fromRight(None, eqExpr))
            continue
        return eqExpr

    if len(childrenExpr) == 1:
        return Right(childrenExpr[0])
    return Right(TreeExpression(TreeOperator.AND, childrenExpr))


def parsePathlessExpression(expr) -> Either[str, MatchableExpression]:
    """
MatchExpr :=  And
            | Nor
            | Or
            | Expr
            ...
    """
    pathlessParser = PathlessExpressions.get(expr.fieldName, None)
    if pathlessParser is None:
        return Left("unknown top level operator: " + expr.fieldName)

    return pathlessParser(expr)

def parseDocumentTopLevel(fieldName: str, expr: BSONElement) -> Either[str, List[MatchableExpression]]:
    """
    This loosely corresponds to parseSub with currentlevel = kUserDocumentTopLevel
    """
    if isGeoExpr(expr.value):
        geoRes = parseGeo(expr)
        if isRight(geoRes):
            return fmap(geoRes, lambda x: [x])
        return geoRes

    res = []
    for field in expr.value.elements:
        parsedField = parseSubField(fieldName, field, expr.value)
        if isLeft(parsedField):
            return parsedField
        res.append(fromRight(None, parsedField))

    return Right(res)


def parseSubField(fieldPath: str, expr: BSONElement, ctx: Optional[BSONDocument]) -> Either[str, MatchableExpression]:
    if expr.fieldName == "$not":
        return parseSubNot(fieldPath, expr)

    opParser = MatchOperatorParser.get(expr.fieldName, None)

    if opParser is None:
        return Left(f"Parser for operator {expr.fieldName} is not defined")

    return opParser(fieldPath, expr, ctx)

def parseSubNot(fieldPath: str, expr: BSONElement) -> Either[str, MatchableExpression]:
    if expr.bsonType == BSONType.Regex:
        return parseRegexMatch(fieldPath, expr) | NotExpression
    if expr.bsonType != BSONType.Document:
        return Left("$not must take a regex or object")
    inner = parseDocumentTopLevel(fieldPath, expr)
    return inner | (lambda x: NotExpression(TreeExpression(TreeOperator.AND, x)))


def parseGeo(expr) -> Either[str, MatchableExpression]:
    return Left("geo is not yet implemented")

def parseRegexMatch(fieldName: str, expr: BSONElement) -> Either[str, MatchableExpression]:
    """
    FieldName : Regex is equivalent to FieldName : {$regex: Regex}
    """
    regexExpr = BSONElement(BSONType.Document, fieldName, BSONDocument([BSONElement(BSONType.Regex, "$regex", expr.value)]))
    print(f"{regexExpr = }")
    return parseSubField(fieldName, regexExpr, None)


def parseTopLevelLogical(opCtor) -> Either[str, MatchableExpression]:
    def _res(expr: BSONElement) -> Either[Any, MatchableExpression]:
        if expr.bsonType != BSONType.Array:
            return Left("Top Level Logical Expression Must Take An Array")

        children = []

        for elm in expr.value.elements:
            if elm.bsonType != BSONType.Document:
                return Left(f"Top Level Logical Array Element Must Be Document, Got: {elm}")
            parsedChild = parsePredicateTopLevel(elm.value)
            if isLeft(parsedChild):
                return parsedChild
            children.append(fromRight(None, parsedChild))

        return Right(opCtor(children))
    return _res

def parseComparison(fieldName: str, expr, operator) -> Either[str, MatchableExpression]:
    if operator != MatchOperator.EQ and expr.bsonType == BSONType.Regex:
        return Left("Regex can only appear in equality comparison")

    return Right(PathMatchExpression(Path.fromString(fieldName), Predicate(operator, expr)))

def parseInArray(fieldName: str, expr: BSONElement) -> Either[str, MatchableExpression]:
    if expr.bsonType != BSONType.Array:
        return Left("$in must take an array")

    for elm in expr.value.elements:
        if isExpressionDocument(elm, False):
            return Left("Cannot have $ operators within $in array")
        # in server a separation of regex value and other literal values happens here
        # but that shouldn't affect the semantics of an $in operator
    return Right(PathMatchExpression(Path.fromString(fieldName), Predicate(MatchOperator.IN, expr)))


defPathless(TreeOperator.AND)(parseTopLevelLogical(lambda children: TreeExpression(TreeOperator.AND, children)))
defPathless(TreeOperator.OR)(parseTopLevelLogical(lambda children: TreeExpression(TreeOperator.OR, children)))
defPathless(TreeOperator.NOR)(parseTopLevelLogical(lambda children: TreeExpression(TreeOperator.NOR, children)))

defMatchOp(MatchOperator.EQ)(lambda fieldName, expr, _: parseComparison(fieldName, expr, MatchOperator.EQ))
defMatchOp(MatchOperator.LTE)(lambda fieldName, expr, _: parseComparison(fieldName, expr, MatchOperator.LTE))
defMatchOp(MatchOperator.LT)(lambda fieldName, expr, _: parseComparison(fieldName, expr, MatchOperator.LT))
defMatchOp(MatchOperator.GT)(lambda fieldName, expr, _: parseComparison(fieldName, expr, MatchOperator.GT))
defMatchOp(MatchOperator.GTE)(lambda fieldName, expr, _: parseComparison(fieldName, expr, MatchOperator.GTE))
defMatchOp(MatchOperator.IN)(lambda fieldName, expr, _: parseInArray(fieldName, expr))
defMatchOp(MatchOperator.NIN)(lambda fieldName, expr, _: parseInArray(fieldName, expr) | NotExpression)
