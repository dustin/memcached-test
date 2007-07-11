#!/usr/bin/env python
"""
A memcached test server.

Copyright (c) 2007  Dustin Sallings <dustin@spy.net>
"""

import asyncore
import socket
import struct

REQ_MAGIC_BYTE = 0xf

PKT_FMT=">BBBxII"
# min recv packet size
MIN_RECV_PACKET = struct.calcsize(PKT_FMT)

# Command constants
CMD_GET = 0
CMD_SET = 1
CMD_ADD = 2
CMD_REPLACE = 3
CMD_DELETE = 4
CMD_INCR = 5
CMD_DECR = 6
CMD_QUIT = 7

SET_PKT_FMT=">II"

EXTRA_HDR_FMTS={
    CMD_SET: SET_PKT_FMT,
    CMD_ADD: SET_PKT_FMT,
    CMD_REPLACE: SET_PKT_FMT
}

class BaseBackend(object):
    """Higher-level backend (processes commands and stuff)."""

    # Command IDs to method names.  This is used to build a dispatch dict on
    # the fly.
    CMDS={CMD_GET: 'handle_get',
        CMD_SET: 'handle_set',
        CMD_ADD: 'handle_add',
        CMD_REPLACE: 'handle_replace',
        CMD_DELETE: 'handle_delete',
        CMD_INCR: 'handle_incr',
        CMD_DECR: 'handle_descr',
        CMD_QUIT: 'handle_quit'
        }

    ERR_UNKNOWN_CMD = 0x81
    ERR_NOT_FOUND = 0x1

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
        assert len(data) > hdrSize + keylen
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

    def handle_unknown(self, cmd, hdrs, key, data):
        """invoked for any unknown command."""
        return self.ERR_UNKNOWN_CMD, "The command %d is unknown" % cmd

class DictBackend(BaseBackend):

    def __init__(self):
        super(DictBackend, self).__init__()
        self.storage={}

    """Sample backend implementation with a non-expiring dict."""
    def handle_get(self, cmd, hdrs, key, data):
        val=self.storage.get(key, None)
        rv=self.ERR_NOT_FOUND, 'Not found'
        if val:
            rv = 0, struct.pack('>I', val[0]) + val[2]
        return rv

    def handle_set(self, cmd, hdrs, key, data):
        self.storage[key]=(hdrs[0], hdrs[1], data)
        print "Stored", self.storage[key], "in", key
        return 0, ''

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
