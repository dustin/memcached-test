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

class Backend(object):
    """Higher-level backend (processes commands and stuff)."""

    # Command constants
    CMD_GET = 0
    CMD_SET = 1
    CMD_ADD = 2
    CMD_REPLACE = 3
    CMD_DELETE = 4
    CMD_INCR = 5
    CMD_DECR = 6
    CMD_QUIT = 7

    def processCommand(self, cmd, key, opaque, data):
        print "Processing command %d with key ``%s'' and data ``%s''" \
            % (cmd, key, data)
        return 0, "Hello!"

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
            rv = len(self.rbuf) - MIN_RECV_PACKET >= remaining + keylen
        return rv

    def handle_read(self):
        self.rbuf += self.recv(self.BUFFER_SIZE)
        while self.__hasEnoughBytes():
            magic, cmd, keylen, opaque, remaining=\
                struct.unpack(PKT_FMT, self.rbuf[:MIN_RECV_PACKET])
            assert magic == REQ_MAGIC_BYTE
            # Grab the data section of this request
            data=self.rbuf[MIN_RECV_PACKET:MIN_RECV_PACKET+remaining+keylen]
            # Remove this request from the read buffer
            self.rbuf=self.rbuf[MIN_RECV_PACKET+remaining+keylen:]
            # Process the command
            cmdVal=self.backend.processCommand(cmd, data[:keylen],
                opaque, data[keylen:])
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
    server = MemcachedServer(Backend(), MemcachedBinaryChannel)
    asyncore.loop()
