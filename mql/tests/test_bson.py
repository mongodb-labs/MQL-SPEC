import unittest

from mql.base.bson import BSONDocument, BSONType
from mql.base.bsonBinary import parseDocument, dumpDocument

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

    def testParseNestedBinary(self):
        nested = [
                14, 0, 0, 0,
                2, 66, 0, 2, 0, 0, 0, 66, 0,
                0
                ]
        raw = [
            22,0,0,0,
            3, 65, 0, *nested,
            0
        ]
        doc = parseDocument(raw)

        self.assertTrue(isRight(doc))

        parsedDoc, rest = fromRight(None, doc)

        self.assertEqual(0, len(rest))

        field = parsedDoc['A']

        self.assertTrue(isJust(field))
        self.assertEqual(BSONType.Document, fromJust(field).value.bsonType)

        nestedDoc = fromJust(field).value.doc()
        self.assertTrue(isJust(nestedDoc))

        nestedField = fromJust(nestedDoc)['B']

        self.assertTrue(isJust(nestedField))
        self.assertEqual(BSONType.String, fromJust(nestedField).value.bsonType)
        self.assertEqual("B\x00", fromJust(nestedField).value.value)

    def testDumpSimpleBson(self):
        bson = BSONDocument.fromDict({"A": "A\x00"})
        dumped = dumpDocument(bson)

        self.assertTrue(isRight(dumped))

        dumpedBytes = fromRight(None, dumped)

        b = [
            14,0,0,0,
            2, 65, 0, 2, 0, 0, 0, 65, 0,
            0
        ]

        self.assertListEqual(dumpedBytes, b)

    def testDumpBuildInfo(self):
        doc = BSONDocument.fromDict({
            "ok": 1,
            "version": "v1.2.3\x00"
            })

        expected = [33,0,0,0,
                16, 111, 107, 0, 1, 0, 0, 0,
                2, 118, 101, 114, 115, 105, 111, 110, 0, 
                7, 0, 0, 0, 118, 49, 46, 50, 46, 51, 0,
                0
                ]

        dumped = dumpDocument(doc)

        self.assertTrue(isRight(dumped))
        self.assertListEqual(fromRight(None, dumped), expected)
