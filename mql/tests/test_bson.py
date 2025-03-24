import unittest

from mql.base.bson import BSONDocument, BSONType
from mql.base.bsonBinary import parseDocument

from fpy.data.maybe import isJust, fromJust
from fpy.data.either import isRight, fromRight

class TestBson(unittest.TestCase):
    def testSimpleFromDict(self):
        raw = {"a": 1}
        doc = BSONDocument.fromDict(raw)

        self.assertEqual(1, len(doc))
        self.assertTrue("a" in doc)

        field = doc['a']
        self.assertTrue(isJust(field))
        self.assertEqual(BSONType.Int32, fromJust(field).value.bsonType)
        self.assertEqual(1, fromJust(field).value.value)

    def testParseSimpleBinary(self):
        raw = [
            14,0,0,0,
            2, 65, 0, 2, 0, 0, 0, 65, 0,
            0
        ]
        doc = parseDocument(raw)

        self.assertTrue(isRight(doc))

        parsedDoc, rest = fromRight(None, doc)

        self.assertEqual(0, len(rest))

        field = parsedDoc['A']

        self.assertTrue(isJust(field))
        self.assertEqual(BSONType.String, fromJust(field).value.bsonType)
        self.assertEqual("A\x00", fromJust(field).value.value)
