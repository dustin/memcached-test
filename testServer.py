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

from memcacheConstants import MIN_RECV_PACKET, PKT_FMT, REQ_MAGIC_BYTE

VERSION="1.0"

EXTRA_HDR_FMTS={
    memcacheConstants.CMD_SET: memcacheConstants.SET_PKT_FMT,
    memcacheConstants.CMD_ADD: memcacheConstants.SET_PKT_FMT,
    memcacheConstants.CMD_REPLACE: memcacheConstants.SET_PKT_FMT
}

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
        memcacheConstants.CMD_DECR: 'handle_descr',
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

    def processCommand(self, cmd, keylen, data):
        """Entry point for command processing.  Lower level protocol
        implementations deliver values here."""
        hdrs, key, val=self._splitKeys(EXTRA_HDR_FMTS.get(cmd, ''),
            keylen, data)

        return self.handlers.get(cmd, self.handle_unknown)(cmd, hdrs, key, val)

    def handle_noop(self, cmd, hdrs, key, data):
        """Handle a noop"""
        print "Noop"
        return 0, ''

    def handle_unknown(self, cmd, hdrs, key, data):
        """invoked for any unknown command."""
        return memcacheConstants.ERR_UNKNOWN_CMD, \
            "The command %d is unknown" % cmd

class DictBackend(BaseBackend):
    """Sample backend implementation with a non-expiring dict."""

    def __init__(self):
        super(DictBackend, self).__init__()
        self.storage={}

    def handle_get(self, cmd, hdrs, key, data):
        val=self.storage.get(key, None)
        rv=memcacheConstants.ERR_NOT_FOUND, 'Not found'
        if val:
            now=time.time()
            if now >= val[1]:
                print key, "expired"
                del self.storage[key]
            else:
                rv = 0, struct.pack('>I', val[0]) + val[2]
                print "Hit looking up", key
        else:
            print "Miss looking up", key
        return rv

    def handle_getq(self, cmd, hdrs, key, data):
        rv=self.handle_get(cmd, hdrs, key, data)
        if rv[0] == memcacheConstants.ERR_NOT_FOUND:
            print "Swallowing miss"
            rv = None
        return rv

    def handle_set(self, cmd, hdrs, key, data):
        self.storage[key]=(hdrs[0], time.time() + hdrs[1], data)
        print "Stored", self.storage[key], "in", key
        return 0, ''

    def handle_add(self, cmd, hdrs, key, data):
        rv=memcacheConstants.ERR_EXISTS, 'Data exists for key'
        if key not in self.storage:
            rv=self.handle_set(cmd, hdrs, key, data)
        return rv

    def handle_replace(self, cmd, hdrs, key, data):
        rv=memcacheConstants.ERR_NOT_FOUND, 'Not found'
        if key in self.storage:
            rv=self.handle_set(cmd, hdrs, key, data)
        return rv

    def handle_flush(self, cmd, hdrs, key, data):
        self.storage.clear()
        print "Flushed"
        return 0, ''

    def handle_delete(self, cmd, hdrs, key, data):
        rv=memcacheConstants.ERR_NOT_FOUND, 'Not found'
        if key in self.storage:
            del self.storage[key]
            print "Deleted", key
            rv=0, ''
        return rv

    def handle_version(self, cmd, hdrs, key, data):
        return 0, "Python test memcached server %s" % VERSION

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
            magic, cmd, keylen, opaque, remaining=\
                struct.unpack(PKT_FMT, self.rbuf[:MIN_RECV_PACKET])
            rv = len(self.rbuf) - MIN_RECV_PACKET >= remaining
        return rv

    def handle_read(self):
        self.rbuf += self.recv(self.BUFFER_SIZE)
        while self.__hasEnoughBytes():
            magic, cmd, keylen, opaque, remaining=\
                struct.unpack(PKT_FMT, self.rbuf[:MIN_RECV_PACKET])
            assert magic == REQ_MAGIC_BYTE
            assert keylen <= remaining
            # Grab the data section of this request
            data=self.rbuf[MIN_RECV_PACKET:MIN_RECV_PACKET+remaining]
            assert len(data) == remaining
            # Remove this request from the read buffer
            self.rbuf=self.rbuf[MIN_RECV_PACKET+remaining:]
            # Process the command
            cmdVal=self.backend.processCommand(cmd, keylen, data)
            # Queue the response to the client if applicable.
            if cmdVal:
                status, response = cmdVal
                self.wbuf += struct.pack(PKT_FMT, REQ_MAGIC_BYTE, cmd, status,
                    opaque, len(response)) + response

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
    server = MemcachedServer(DictBackend(), MemcachedBinaryChannel)
    asyncore.loop()
