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
CMD_QUIT = 6
CMD_FLUSH = 7
CMD_GETQ = 8
CMD_NOOP = 9
CMD_VERSION = 10

CMD_DECR = 11

CMD_GETS = 50
CMD_CAS = 51

# Flags, expiration
SET_PKT_FMT=">II"
# flags, expiration, id
CAS_PKT_FMT=">IIQ"

# flags
GET_RES_FMT=">IQ"

# How long until the deletion takes effect.
DEL_PKT_FMT=">I"

# amount, initial value, expiration
INCRDECR_PKT_FMT=">QQI"
# Special incr expiration that means do not store
INCRDECR_SPECIAL=0xffffffff

REQ_MAGIC_BYTE = 0x0f
RES_MAGIC_BYTE = 0xf0

PKT_FMT=">BBHIII"
# min recv packet size
MIN_RECV_PACKET = struct.calcsize(PKT_FMT)

ERR_UNKNOWN_CMD = 0x81
ERR_NOT_FOUND = 0x1
ERR_EXISTS = 0x2
