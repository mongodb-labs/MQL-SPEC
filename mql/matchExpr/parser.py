from fpy.parsec.parsec import parser
from fpy.data.either import Left, Right, isLeft, isRight, fromLeft, fromRight, Either
from fpy.composable.function import func
from fpy.control.functor import fmap
from typing import Any, List, Dict, Callable
from mql.matchExpr.querySelector import MatchOperator, Predicate, PathMatchExpression, TreeOperator, TreeExpression, MatchableExpression, OperatorArity, OperatorKW
from mql.base.bson import BSONElement, BSONDocument, BSONType
from mql.base.path import Path

PathlessExpressions: Dict[str, Callable[[BSONElement], Either[Any, Any]]] = dict()

def defPathless(name):
    global PathlessExpressions
    def res(fn):
        PathlessExpressions[name] = fn
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
            subExprs = parseDocumentTopLevel(elm)
            if isRight(subExprs):
                childrenExpr.extend(fromRight([], subExprs))
                continue
            return subExprs
        if elm.bsonType == BSONType.Regex:
            regexExpr = parseRegexMatch(elm)
            if isRight(regexExpr):
                childrenExpr.append(fromRight(None, regexExpr))
                continue
            return regexExpr
        # default case is field equality
        eqExpr = parseImplicitEq(elm)
        if isRight(eqExpr):
            childrenExpr.append(fromRight(None, eqExpr))
            continue
        return eqExpr

    if len(childrenExpr) == 1:
        return Right(childrenExpr[0])
    return Right(TreeExpression(TreeOperator.AND, childrenExpr))


def parsePathlessExpression(expr) -> Either[Any, Any]:
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

def parseDocumentTopLevel(expr: BSONElement) -> Either[str, List[MatchableExpression]]:
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
        parsedField = parseSubField(expr.fieldName, field)
        if isLeft(parsedField):
            return parsedField
        res.append(fromRight(None, parsedField))

    return Right(res)


def parseUserSubDocument(expr) -> Either[str, List[MatchableExpression]]:
    pass

def parseSubField(fieldPath: str, expr: BSONElement) -> Either[str, MatchableExpression]:
    if expr.fieldName == "$not":
        return parseSubNot(fieldPath, expr)
    # print(f"operator: {expr.fieldName}")
    exprOp = None
    for op in MatchOperator:
        if op.value == expr.fieldName:
            exprOp = op

    if exprOp is None:
        return Left(f"Operator {expr.fieldName} is not defined")

    return Right(PathMatchExpression(Path.fromString(fieldPath), Predicate(exprOp, expr)))
    

def parseSubNot(fieldPath: str, expr: BSONElement):
    pass

def parseGeo(expr):
    pass

def parseRegexMatch(expr) -> Either[str, MatchableExpression]:
    """
    FieldName : Regex is equivalent to FieldName : {$regex: Regex}
    """
    regexExpr = BSONElement(BSONType.Document, expr.fieldName, BSONDocument([BSONElement(BSONType.Regex, "$regex", expr.value)]))
    print(f"{regexExpr = }")
    return parseSubField(expr.fieldName, regexExpr)

def parseImplicitEq(expr):
    return parseComparison(expr.fieldName, expr, MatchOperator.EQ)

def parseComparison(fieldName: str, expr, operator) -> Either[str, MatchableExpression]:
    if operator != MatchOperator.EQ and expr.bsonType == BSONType.Regex:
        return Left("Regex can only appear in equality comparison")

    return Right(PathMatchExpression(Path.fromString(fieldName), Predicate(operator, expr)))


def parseTopLevelLogical(opCtor):
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

defPathless("$and")(parseTopLevelLogical(lambda children: TreeExpression(TreeOperator.AND, children)))
defPathless("$or")(parseTopLevelLogical(lambda children: TreeExpression(TreeOperator.OR, children)))
defPathless("$nor")(parseTopLevelLogical(lambda children: TreeExpression(TreeOperator.NOR, children)))
