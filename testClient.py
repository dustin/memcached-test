#!/usr/bin/env python
"""
Binary memcached test client.

Copyright (c) 2007  Dustin Sallings <dustin@spy.net>
"""

import sys
import socket
import random
import struct
import exceptions

from testServer import REQ_MAGIC_BYTE, PKT_FMT, MIN_RECV_PACKET, SET_PKT_FMT
import testServer

class MemcachedError(exceptions.Exception):
    """Error raised when a command fails."""
    pass

class MemcachedClient(object):
    """Simple memcached client."""

    def __init__(self, host='127.0.0.1', port=11211):
        self.s=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.connect_ex((host, port))
        self.r=random.Random()

    def _sendCmd(self, cmd, key, val, extraHeader=''):
        """Send a command and await its response."""
        myopaque=self.r.randint(0, 2**32)
        msg=struct.pack(PKT_FMT, REQ_MAGIC_BYTE,
            cmd, len(key), myopaque, len(key) + len(extraHeader) + len(val))
        self.s.send(msg + extraHeader + key + val)
        response=self.s.recv(MIN_RECV_PACKET)
        assert len(response) == MIN_RECV_PACKET
        magic, cmd, errcode, opaque, remaining=struct.unpack(PKT_FMT, response)
        rv=self.s.recv(remaining)
        assert magic == REQ_MAGIC_BYTE
        assert opaque == myopaque
        if errcode != 0:
            raise MemcachedError("Error:  " + `errcode` + ": " +  rv)
        return rv

    def _mutate(self, cmd, key, exp, flags, val):
        self._sendCmd(cmd, key, val, struct.pack(SET_PKT_FMT, flags, exp))

    def set(self, key, exp, flags, val):
        """Set a value in the memcached server."""
        self._mutate(testServer.CMD_SET, key, exp, flags, val)

    def add(self, key, exp, flags, val):
        """Add a value in the memcached server iff it doesn't already exist."""
        self._mutate(testServer.CMD_ADD, key, exp, flags, val)

    def replace(self, key, exp, flags, val):
        """Replace a value in the memcached server iff it already exists."""
        self._mutate(testServer.CMD_REPLACE, key, exp, flags, val)

    def get(self, key):
        """Get the value for a given key within the memcached server."""
        parts=self._sendCmd(testServer.CMD_GET, key, '')
        return struct.unpack(">I", parts[:4])[0], parts[4:]

    def delete(self, key):
        """Delete the value for a given key within the memcached server."""
        self._sendCmd(testServer.CMD_DELETE, key, '')

    def flush(self):
        """Flush all storage in a memcached instance."""
        self._sendCmd(testServer.CMD_FLUSH, '', '')
