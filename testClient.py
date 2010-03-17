#!/usr/bin/env python
"""
Binary memcached test client.

Copyright (c) 2007  Dustin Sallings <dustin@spy.net>
"""

import sys
import time
import hmac
import socket
import random
import struct
import exceptions

import unittest

import memcacheConstants
from mc_bin_client import MemcachedClient, MemcachedError

class ComplianceTest(unittest.TestCase):

    def setUp(self):
        self.mc=MemcachedClient()
        self.mc.flush()

    def tearDown(self):
        self.mc.flush()
        self.mc.close()

    def testVersion(self):
        """Test the version command returns something."""
        v=self.mc.version()
        self.assertTrue(len(v) > 0, "Bad version:  ``" + str(v) + "''")

    def testSimpleSetGet(self):
        """Test a simple set and get."""
        self.mc.set("x", 5, 19, "somevalue")
        self.assertGet((19, "somevalue"), self.mc.get("x"))

    def testZeroExpiration(self):
        """Ensure zero-expiration sets work properly."""
        self.mc.set("x", 0, 19, "somevalue")
        time.sleep(1.1)
        self.assertGet((19, "somevalue"), self.mc.get("x"))

    def assertNotExists(self, key):
        try:
            x=self.mc.get(key)
            self.fail("Expected an exception, got " + `x`)
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_NOT_FOUND, e.status)

    def assertGet(self, exp, gv):
        self.assertTrue(gv is not None)
        self.assertEquals((gv[0], gv[2]), exp)

    def testDelete(self):
        """Test a set, get, delete, get sequence."""
        self.mc.set("x", 5, 19, "somevalue")
        self.assertGet((19, "somevalue"), self.mc.get("x"))
        self.mc.delete("x")
        self.assertNotExists("x")

    def testFlush(self):
        """Test flushing."""
        self.mc.set("x", 5, 19, "somevaluex")
        self.mc.set("y", 5, 17, "somevaluey")
        self.assertGet((19, "somevaluex"), self.mc.get("x"))
        self.assertGet((17, "somevaluey"), self.mc.get("y"))
        self.mc.flush()
        self.assertNotExists("x")
        self.assertNotExists("y")

    def testNoop(self):
        """Making sure noop is understood."""
        self.mc.noop()

    def testAdd(self):
        """Test add functionality."""
        self.assertNotExists("x")
        self.mc.add("x", 5, 19, "ex")
        self.assertGet((19, "ex"), self.mc.get("x"))
        try:
            self.mc.add("x", 5, 19, "ex2")
            self.fail("Expected failure to add existing key")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_EXISTS, e.status)
        self.assertGet((19, "ex"), self.mc.get("x"))

    def testReplace(self):
        """Test replace functionality."""
        self.assertNotExists("x")
        try:
            self.mc.replace("x", 5, 19, "ex")
            self.fail("Expected failure to replace missing key")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_NOT_FOUND, e.status)
        self.mc.add("x", 5, 19, "ex")
        self.assertGet((19, "ex"), self.mc.get("x"))
        self.mc.replace("x", 5, 19, "ex2")
        self.assertGet((19, "ex2"), self.mc.get("x"))

    def testMultiGet(self):
        """Testing multiget functionality"""
        self.mc.add("x", 5, 1, "ex")
        self.mc.add("y", 5, 2, "why")
        vals=self.mc.getMulti('xyz')
        self.assertGet((1, 'ex'), vals['x'])
        self.assertGet((2, 'why'), vals['y'])
        self.assertEquals(2, len(vals))

    def testIncrDoesntExistNoCreate(self):
        """Testing incr when a value doesn't exist (and not creating)."""
        try:
            self.mc.incr("x", exp=memcacheConstants.INCRDECR_SPECIAL)
            self.fail("Expected failure to increment non-existent key")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_NOT_FOUND, e.status)
        self.assertNotExists("x")

    def testIncrDoesntExistCreate(self):
        """Testing incr when a value doesn't exist (and we make a new one)"""
        self.assertNotExists("x")
        self.assertEquals(19, self.mc.incr("x", init=19)[0])

    def testDecrDoesntExistNoCreate(self):
        """Testing decr when a value doesn't exist (and not creating)."""
        try:
            self.mc.decr("x", exp=memcacheConstants.INCRDECR_SPECIAL)
            self.fail("Expected failiure to decrement non-existent key.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_NOT_FOUND, e.status)
        self.assertNotExists("x")

    def testDecrDoesntExistCreate(self):
        """Testing decr when a value doesn't exist (and we make a new one)"""
        self.assertNotExists("x")
        self.assertEquals(19, self.mc.decr("x", init=19)[0])

    def testIncr(self):
        """Simple incr test."""
        val, cas=self.mc.incr("x")
        self.assertEquals(0, val)
        val, cas=self.mc.incr("x")
        self.assertEquals(1, val)
        val, cas=self.mc.incr("x", 211)
        self.assertEquals(212, val)
        val, cas=self.mc.incr("x", 2**33)
        self.assertEquals(8589934804L, val)

    def testDecr(self):
        """Simple decr test."""
        val, cas=self.mc.incr("x", init=5)
        self.assertEquals(5, val)
        val, cas=self.mc.decr("x")
        self.assertEquals(4, val)
        val, cas=self.mc.decr("x", 211)
        self.assertEquals(0, val)

    def testCas(self):
        """Test CAS operation."""
        try:
            self.mc.cas("x", 5, 19, 0x7fffffffff, "bad value")
            self.fail("Expected error CASing with no existing value")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_NOT_FOUND, e.status)
        self.mc.add("x", 5, 19, "original value")
        flags, i, val=self.mc.get("x")
        self.assertEquals("original value", val)
        try:
            self.mc.cas("x", 5, 19, i+1, "broken value")
            self.fail("Expected error CASing with invalid id")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_EXISTS, e.status)
        self.mc.cas("x", 5, 19, i, "new value")
        newflags, newi, newval=self.mc.get("x")
        self.assertEquals("new value", newval)

        # Test a CAS replay
        try:
            self.mc.cas("x", 5, 19, i, "crap value")
            self.fail("Expected error CASing with invalid id")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_EXISTS, e.status)
        newflags, newi, newval=self.mc.get("x")
        self.assertEquals("new value", newval)

    # Assert we know the correct CAS for a given key.
    def assertValidCas(self, key, cas):
        flags, currentcas, val=self.mc.get(key)
        self.assertEquals(currentcas, cas)

    def testSetReturnsCas(self):
        """Ensure a set command returns the current CAS."""
        vals=self.mc.set('x', 5, 19, 'some val')
        self.assertValidCas('x', vals[1])

    def testAddReturnsCas(self):
        """Ensure an add command returns the current CAS."""
        vals=self.mc.add('x', 5, 19, 'some val')
        self.assertValidCas('x', vals[1])

    def testReplaceReturnsCas(self):
        """Ensure a replace command returns the current CAS."""
        vals=self.mc.add('x', 5, 19, 'some val')
        vals=self.mc.replace('x', 5, 19, 'other val')
        self.assertValidCas('x', vals[1])

    def testIncrReturnsCAS(self):
        """Ensure an incr command returns the current CAS."""
        val, cas, something=self.mc.set("x", 5, 19, '4')
        val, cas=self.mc.incr("x", init=5)
        self.assertEquals(5, val)
        self.assertValidCas('x', cas)

    def testDecrReturnsCAS(self):
        """Ensure an decr command returns the current CAS."""
        val, cas, something=self.mc.set("x", 5, 19, '4')
        val, cas=self.mc.decr("x", init=5)
        self.assertEquals(3, val)
        self.assertValidCas('x', cas)

    def testDeletionCAS(self):
        """Validation deletion honors cas."""
        try:
            self.mc.delete("x")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_NOT_FOUND, e.status)
        val, cas, something=self.mc.set("x", 5, 19, '4')
        try:
            self.mc.delete('x', cas=cas+1)
            self.fail("Deletion should've failed.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_EXISTS, e.status)
        self.assertGet((19, '4'), self.mc.get('x'))
        self.mc.delete('x', cas=cas)
        self.assertNotExists('x')

    def testAppend(self):
        """Test append functionality."""
        val, cas, something=self.mc.set("x", 5, 19, "some")
        val, cas, something=self.mc.append("x", "thing")
        self.assertGet((19, 'something'), self.mc.get("x"))

    def testAppendCAS(self):
        """Test append functionality honors CAS."""
        val, cas, something=self.mc.set("x", 5, 19, "some")
        try:
            val, cas, something=self.mc.append("x", "thing", cas+1)
            self.fail("expected CAS failure.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_EXISTS, e.status)
        self.assertGet((19, 'some'), self.mc.get("x"))

    def testPrepend(self):
        """Test prepend functionality."""
        val, cas, something=self.mc.set("x", 5, 19, "some")
        val, cas, something=self.mc.prepend("x", "thing")
        self.assertGet((19, 'thingsome'), self.mc.get("x"))

    def testPrependCAS(self):
        """Test prepend functionality honors CAS."""
        val, cas, something=self.mc.set("x", 5, 19, "some")
        try:
            val, cas, something=self.mc.prepend("x", "thing", cas+1)
            self.fail("expected CAS failure.")
        except MemcachedError, e:
            self.assertEquals(memcacheConstants.ERR_EXISTS, e.status)
        self.assertGet((19, 'some'), self.mc.get("x"))

    def testTimeBombedFlush(self):
        """Test a flush with a time bomb."""
        val, cas, something=self.mc.set("x", 5, 19, "some")
        self.mc.flush(2)
        self.assertGet((19, 'some'), self.mc.get("x"))
        time.sleep(2.1)
        self.assertNotExists('x')

if __name__ == '__main__':
    unittest.main()
