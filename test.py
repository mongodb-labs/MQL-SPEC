from mql.base.bson import BSONElement, BSONDocument, BSONArray, BSONType
from mql.base.path import Path
from mql.matchExpr.querySelector import MatchOperator, Predicate, PathMatchExpression
from mql.matchExpr.parser import parsePredicateTopLevel

from fpy.data.either import fromRight, isLeft

if __name__ == "__main__":
    rawQuery = {"a": 1,
                "b": {"$gt": 1}}
    
    parsedQuery = parsePredicateTopLevel(BSONDocument.fromDict(rawQuery))
    
    if isLeft(parsedQuery):
        print(parsedQuery)
    
    else:
        query = fromRight(None, parsedQuery)
        
        print("Input Query")
        print(rawQuery)
        print("Parsed Query")
        print(query)
        
        docs = [{"a": 1},
                {"b": 2},
                {"a": 1, "b": 2},
                {"a": 1, "c": 3},
                {"a": 1, "b": 3, "c": 3},
                {"a": 1, "d": {"e": 4}}
                ]
        
        for doc in docs:
            bdoc = BSONDocument.fromDict(doc)
            print(">>> Evaluating on doc:")
            print(f"{doc = }")
            print(query.matches(bdoc))
            print("<<<")
