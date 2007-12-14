#!/usr/bin/env python
"""

Copyright (c) 2007  Dustin Sallings <dustin@spy.net>
"""

import struct

# Command constants
CMD_GET = 0
CMD_SET = 1
CMD_ADD = 2
CMD_REPLACE = 3
CMD_DELETE = 4
CMD_INCR = 5
CMD_DECR = 6
CMD_QUIT = 7
CMD_FLUSH = 8
CMD_GETQ = 9
CMD_NOOP = 10
CMD_VERSION = 11

# Flags, expiration, cas ID
SET_PKT_FMT=">IIQ"
# flags, expiration, id
CAS_PKT_FMT=">IIQ"

# flags, cas ID
GET_RES_FMT=">IQ"

# How long until the deletion takes effect.
DEL_PKT_FMT=">I"

# amount, initial value, expiration
INCRDECR_PKT_FMT=">QQI"
# Special incr expiration that means do not store
INCRDECR_SPECIAL=0xffffffff

MAGIC_BYTE = 0x80
REQ_MAGIC_BYTE = MAGIC_BYTE
RES_MAGIC_BYTE = MAGIC_BYTE

PKT_FMT=">BBHIII"
# min recv packet size
MIN_RECV_PACKET = struct.calcsize(PKT_FMT)

ERR_UNKNOWN_CMD = 0x81
ERR_NOT_FOUND = 0x1
ERR_EXISTS = 0x2
