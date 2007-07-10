#!/usr/bin/env python
"""
Binary memcached test client.

Copyright (c) 2007  Dustin Sallings <dustin@spy.net>
"""

import sys
import socket
import random
import struct

from testServer import REQ_MAGIC_BYTE, PKT_FMT, MIN_RECV_PACKET

if __name__ == '__main__':
    s=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    rv = s.connect_ex(('127.0.0.1', 11211))

    cmd, key, val = sys.argv[1:]

    myopaque=random.Random().randint(0, 2**32)
    s.send(struct.pack(PKT_FMT, REQ_MAGIC_BYTE,
        int(cmd), len(key), myopaque, len(val)) + key + val)
    response=s.recv(MIN_RECV_PACKET)
    assert len(response) == MIN_RECV_PACKET
    magic, cmd, errcode, opaque, remaining=struct.unpack(PKT_FMT, response)
    assert magic == REQ_MAGIC_BYTE
    assert opaque == myopaque
    print "Error code: ", errcode
    print s.recv(remaining)
