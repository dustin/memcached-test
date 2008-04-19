#!/usr/bin/env python
"""
A memcached test server.

Copyright (c) 2007  Dustin Sallings <dustin@spy.net>
"""

import asyncore
import socket
import struct
import time

import memcacheConstants

from memcacheConstants import MIN_RECV_PACKET, REQ_PKT_FMT, RES_PKT_FMT
from memcacheConstants import INCRDECR_RES_FMT
from memcacheConstants import REQ_MAGIC_BYTE, RES_MAGIC_BYTE, EXTRA_HDR_FMTS

VERSION="1.0"

class BaseBackend(object):
    """Higher-level backend (processes commands and stuff)."""

    # Command IDs to method names.  This is used to build a dispatch dict on
    # the fly.
    CMDS={
        memcacheConstants.CMD_GET: 'handle_get',
        memcacheConstants.CMD_GETQ: 'handle_getq',
        memcacheConstants.CMD_SET: 'handle_set',
        memcacheConstants.CMD_ADD: 'handle_add',
        memcacheConstants.CMD_REPLACE: 'handle_replace',
        memcacheConstants.CMD_DELETE: 'handle_delete',
        memcacheConstants.CMD_INCR: 'handle_incr',
        memcacheConstants.CMD_DECR: 'handle_decr',
        memcacheConstants.CMD_QUIT: 'handle_quit',
        memcacheConstants.CMD_FLUSH: 'handle_flush',
        memcacheConstants.CMD_NOOP: 'handle_noop',
        memcacheConstants.CMD_VERSION: 'handle_version',
        }

    def __init__(self):
        self.handlers={}

        for id, method in self.CMDS.iteritems():
            self.handlers[id]=getattr(self, method, self.handle_unknown)

    def _splitKeys(self, fmt, keylen, data):
        """Split the given data into the headers as specified in the given
        format, the key, and the data.

        Return (hdrTuple, key, data)"""
        hdrSize=struct.calcsize(fmt)
        assert hdrSize <= len(data), "Data too short for " + fmt + ': ' + `data`
        hdr=struct.unpack(fmt, data[:hdrSize])
        assert len(data) >= hdrSize + keylen
        key=data[hdrSize:keylen+hdrSize]
        assert len(key) == keylen, "len(%s) == %d, expected %d" \
            % (key, len(key), keylen)
        val=data[keylen+hdrSize:]
        return hdr, key, val

    def _error(self, which, msg):
        return which, 0, msg

    def processCommand(self, cmd, keylen, cas, data):
        """Entry point for command processing.  Lower level protocol
        implementations deliver values here."""
        hdrs, key, val=self._splitKeys(EXTRA_HDR_FMTS.get(cmd, ''),
            keylen, data)

        return self.handlers.get(cmd, self.handle_unknown)(cmd, hdrs, key,
            cas, val)

    def handle_noop(self, cmd, hdrs, key, cas, data):
        """Handle a noop"""
        print "Noop"
        return 0, 0, ''

    def handle_unknown(self, cmd, hdrs, key, cas, data):
        """invoked for any unknown command."""
        return self._error(memcacheConstants.ERR_UNKNOWN_CMD,
            "The command %d is unknown" % cmd)

class DictBackend(BaseBackend):
    """Sample backend implementation with a non-expiring dict."""

    def __init__(self):
        super(DictBackend, self).__init__()
        self.storage={}
        self.held_keys={}

    def __lookup(self, key):
        rv=self.storage.get(key, None)
        if rv:
            now=time.time()
            if now >= rv[1]:
                print key, "expired"
                del self.storage[key]
                rv=None
        else:
            print "Miss looking up", key
        return rv

    def handle_get(self, cmd, hdrs, key, cas, data):
        val=self.__lookup(key)
        if val:
            rv = 0, id(val), struct.pack(
                memcacheConstants.GET_RES_FMT, val[0]) + str(val[2])
        else:
            rv=self._error(memcacheConstants.ERR_NOT_FOUND, 'Not found')
        return rv

    def handle_set(self, cmd, hdrs, key, cas, data):
        print "Handling a set with", hdrs
        val=self.__lookup(key)
        oldVal = cas
        exp, flags=hdrs
        if oldVal == 0 or (val and oldVal == id(val)):
            rv = self.__handle_unconditional_set(cmd, hdrs, key, data)
        elif val:
            rv = self._error(memcacheConstants.ERR_EXISTS, 'Exists')
        else:
            rv = self._error(memcacheConstants.ERR_NOT_FOUND, 'Not found')
        return rv

    def handle_getq(self, cmd, hdrs, key, cas, data):
        rv=self.handle_get(cmd, hdrs, key, cas, data)
        if rv[0] == memcacheConstants.ERR_NOT_FOUND:
            print "Swallowing miss"
            rv = None
        return rv

    def __handle_unconditional_set(self, cmd, hdrs, key, data):
        self.storage[key]=(hdrs[0], time.time() + hdrs[1], data)
        print "Stored", self.storage[key], "in", key
        if key in self.held_keys:
            del self.held_keys[key]
        return 0, id(self.storage[key]), ''

    def __mutation(self, cmd, hdrs, key, data, multiplier):
        amount, initial, expiration=hdrs
        rv=self._error(memcacheConstants.ERR_NOT_FOUND, 'Not found')
        val=self.storage.get(key, None)
        print "Mutating %s, hdrs=%s, val=%s %s" % (key, `hdrs`, `val`,
            multiplier)
        if val:
            val = (val[0], val[1], max(0, long(val[2]) + (multiplier * amount)))
            self.storage[key]=val
            rv=0, id(val), str(val[2])
        else:
            if expiration != memcacheConstants.INCRDECR_SPECIAL:
                self.storage[key]=(0, time.time() + expiration, initial)
                rv=0, id(self.storage[key]), str(initial)
        if rv[0] == 0:
            rv = rv[0], rv[1], struct.pack(
                memcacheConstants.INCRDECR_RES_FMT, long(rv[2]))
        print "Returning", rv
        return rv

    def handle_incr(self, cmd, hdrs, key, cas, data):
        return self.__mutation(cmd, hdrs, key, data, 1)

    def handle_decr(self, cmd, hdrs, key, cas, data):
        return self.__mutation(cmd, hdrs, key, data, -1)

    def __has_hold(self, key):
        rv=False
        now=time.time()
        print "Looking for hold of", key, "in", self.held_keys, "as of", now
        if key in self.held_keys:
            if time.time() > self.held_keys[key]:
                del self.held_keys[key]
            else:
                rv=True
        return rv

    def handle_add(self, cmd, hdrs, key, cas, data):
        rv=self._error(memcacheConstants.ERR_EXISTS, 'Data exists for key')
        if key not in self.storage and not self.__has_hold(key):
            rv=self.__handle_unconditional_set(cmd, hdrs, key, data)
        return rv

    def handle_replace(self, cmd, hdrs, key, cas, data):
        rv=self._error(memcacheConstants.ERR_NOT_FOUND, 'Not found')
        if key in self.storage and not self.__has_hold(key):
            rv=self.__handle_unconditional_set(cmd, hdrs, key, data)
        return rv

    def handle_flush(self, cmd, hdrs, key, cas, data):
        self.storage.clear()
        self.held_keys.clear()
        print "Flushed"
        return 0, 0, ''

    def handle_delete(self, cmd, hdrs, key, cas, data):
        rv=self._error(memcacheConstants.ERR_NOT_FOUND, 'Not found')
        if key in self.storage:
            del self.storage[key]
            print "Deleted", key, hdrs[0]
            if hdrs[0] > 0:
                self.held_keys[key] = time.time() + hdrs[0]
            rv=0, 0, ''
        return rv

    def handle_version(self, cmd, hdrs, key, cas, data):
        return 0, 0, "Python test memcached server %s" % VERSION

class MemcachedBinaryChannel(asyncore.dispatcher):
    """A channel implementing the binary protocol for memcached."""

    # Receive buffer size
    BUFFER_SIZE = 4096

    def __init__(self, channel, backend):
        asyncore.dispatcher.__init__(self, channel)
        self.log_info("New bin connection from %s" % str(self.addr))
        self.backend=backend
        self.wbuf=""
        self.rbuf=""

    def __hasEnoughBytes(self):
        rv=False
        if len(self.rbuf) >= MIN_RECV_PACKET:
            magic, cmd, keylen, extralen, datatype, remaining, opaque, cas=\
                struct.unpack(REQ_PKT_FMT, self.rbuf[:MIN_RECV_PACKET])
            rv = len(self.rbuf) - MIN_RECV_PACKET >= remaining
        return rv

    def handle_read(self):
        self.rbuf += self.recv(self.BUFFER_SIZE)
        while self.__hasEnoughBytes():
            magic, cmd, keylen, extralen, datatype, remaining, opaque, cas=\
                struct.unpack(REQ_PKT_FMT, self.rbuf[:MIN_RECV_PACKET])
            assert magic == REQ_MAGIC_BYTE
            assert keylen <= remaining, "Keylen is too big: %d > %d" \
                % (keylen, remaining)
            assert extralen == memcacheConstants.EXTRA_HDR_SIZES.get(cmd, 0)
            # Grab the data section of this request
            data=self.rbuf[MIN_RECV_PACKET:MIN_RECV_PACKET+remaining]
            assert len(data) == remaining
            # Remove this request from the read buffer
            self.rbuf=self.rbuf[MIN_RECV_PACKET+remaining:]
            # Process the command
            cmdVal=self.backend.processCommand(cmd, keylen, cas, data)
            # Queue the response to the client if applicable.
            if cmdVal:
                try:
                    status, cas, response = cmdVal
                except ValueError:
                    print "Got", cmdVal
                    raise
                dtype=0
                extralen=memcacheConstants.EXTRA_HDR_SIZES.get(cmd, 0)
                self.wbuf += struct.pack(RES_PKT_FMT,
                    RES_MAGIC_BYTE, cmd, keylen,
                    extralen, dtype, status,
                    len(response), opaque, cas) + response

    def writable(self):
        return self.wbuf

    def handle_write(self):
        sent = self.send(self.wbuf)
        self.wbuf = self.wbuf[sent:]

    def handle_close(self):
        self.log_info("Disconnected from %s" % str(self.addr))
        self.close()

class MemcachedServer(asyncore.dispatcher):
    """A memcached server."""
    def __init__(self, backend, handler, port=11211):
        asyncore.dispatcher.__init__(self)

        self.handler=handler
        self.backend=backend

        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.set_reuse_addr()
        self.bind(("", port))
        self.listen(5)
        self.log_info("Listening on %d" % port)

    def handle_accept(self):
        channel, addr = self.accept()
        self.handler(channel, self.backend)

if __name__ == '__main__':
    server = MemcachedServer(DictBackend(), MemcachedBinaryChannel, port=11211)
    asyncore.loop()
