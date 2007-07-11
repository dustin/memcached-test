#!/usr/bin/env python
"""
Binary memcached test client.

Copyright (c) 2007  Dustin Sallings <dustin@spy.net>
"""

import sys
import socket
import random
import struct

from testServer import REQ_MAGIC_BYTE, PKT_FMT, MIN_RECV_PACKET, EXTRA_HDR_FMTS
from testServer import CMD_SET, CMD_ADD, CMD_REPLACE

if __name__ == '__main__':

    s=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    rv = s.connect_ex(('127.0.0.1', 11211))

    cmd, key, val = sys.argv[1:4]
    cmd = int(cmd)
    if cmd in EXTRA_HDR_FMTS:
        flags, exp=[int(x) for x in sys.argv[4:]]

    myopaque=random.Random().randint(0, 2**32)

    extraHeader=''
    if cmd in EXTRA_HDR_FMTS:
        extraHeader = struct.pack(">II", flags, exp)

    msg=struct.pack(PKT_FMT, REQ_MAGIC_BYTE,
        cmd, len(key), myopaque, len(key) + len(extraHeader) + len(val))
    s.send(msg + extraHeader + key + val)
    response=s.recv(MIN_RECV_PACKET)
    assert len(response) == MIN_RECV_PACKET
    magic, cmd, errcode, opaque, remaining=struct.unpack(PKT_FMT, response)
    assert magic == REQ_MAGIC_BYTE
    assert opaque == myopaque
    print "Error code: ", errcode
    print `s.recv(remaining)`
