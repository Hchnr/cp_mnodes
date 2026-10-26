"""Read-only Linux UNIX_DIAG queue observations; never read IPC payloads.

UAPI: https://github.com/torvalds/linux/blob/master/include/uapi/linux/unix_diag.h
Queue meanings: https://github.com/torvalds/linux/blob/master/net/unix/diag.c
Missing, interrupted, malformed or unavailable observations are errors, never
evidence that a source reader's output has drained.
"""
import os
import socket
import struct
import time


_NL_HEADER = struct.Struct('=IHHII')
_UNIX_MESSAGE = struct.Struct('=BBBBIII')
_NETLINK_SOCK_DIAG = 4
_SOCK_DIAG_BY_FAMILY = 20
_NLM_F_REQUEST_DUMP = 0x301
_NLM_F_DUMP_INTR = 0x10


def _messages(data):
    pos = 0
    while pos < len(data):
        if len(data) - pos < _NL_HEADER.size:
            raise ValueError('truncated netlink header')
        length, kind, flags, sequence, port = _NL_HEADER.unpack_from(data, pos)
        if length < _NL_HEADER.size or pos + length > len(data):
            raise ValueError('invalid netlink message length')
        yield kind, flags, sequence, data[pos + _NL_HEADER.size:pos + length]
        pos += (length + 3) & ~3
    if pos != len(data):
        raise ValueError('truncated netlink alignment')


def _unix_message(body):
    if len(body) < _UNIX_MESSAGE.size:
        raise ValueError('truncated UNIX_DIAG message')
    family, kind, state, _, inode, _, _ = _UNIX_MESSAGE.unpack_from(body)
    if family != socket.AF_UNIX:
        raise ValueError('unexpected diagnostic address family')
    value = {'inode': inode, 'type': kind, 'state': state}
    pos = _UNIX_MESSAGE.size
    while pos < len(body):
        if len(body) - pos < 4:
            raise ValueError('truncated UNIX_DIAG attribute')
        length, attribute = struct.unpack_from('=HH', body, pos)
        if length < 4 or pos + length > len(body):
            raise ValueError('invalid UNIX_DIAG attribute length')
        payload = body[pos + 4:pos + length]
        attribute &= 0x3fff
        if attribute == 2:  # UNIX_DIAG_PEER
            if len(payload) != 4:
                raise ValueError('invalid UNIX_DIAG peer')
            value['peer'] = struct.unpack('=I', payload)[0]
        elif attribute == 4:  # UNIX_DIAG_RQLEN
            if len(payload) != 8:
                raise ValueError('invalid UNIX_DIAG queue lengths')
            value['receive_bytes'], value['send_bytes'] = struct.unpack('=II', payload)
        pos += (length + 3) & ~3
    if pos != len(body) or 'receive_bytes' not in value:
        raise ValueError('missing or malformed UNIX_DIAG queue observation')
    return value


def unix_queues(inodes=None, timeout=3):
    """Observe peer/queue metadata in this network namespace, not message data."""
    if not hasattr(socket, 'AF_NETLINK'):
        raise OSError('Linux socket diagnostics unavailable')
    selected = None if inodes is None else set(inodes)
    sequence = time.monotonic_ns() & 0xffffffff
    # PEER | RQLEN; intentionally omit socket names and memory/payload data.
    request = struct.pack('=BBHIIIII', socket.AF_UNIX, 0, 0,
                          0xffffffff, 0, 0x14, 0xffffffff, 0xffffffff)
    deadline = time.monotonic() + timeout
    result, received = {}, 0
    with socket.socket(socket.AF_NETLINK, socket.SOCK_RAW, _NETLINK_SOCK_DIAG) as channel:
        channel.bind((0, 0))
        port = channel.getsockname()[0]
        message = _NL_HEADER.pack(_NL_HEADER.size + len(request), _SOCK_DIAG_BY_FAMILY,
                                  _NLM_F_REQUEST_DUMP, sequence, port) + request
        channel.sendto(message, (0, 0))
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError('UNIX_DIAG observation timed out')
            channel.settimeout(remaining)
            data, _, flags, address = channel.recvmsg(262144)
            if not data or flags & socket.MSG_TRUNC or address[0] != 0:
                raise ValueError('truncated or non-kernel UNIX_DIAG observation')
            received += len(data)
            if received > 16 * 1024**2:
                raise ValueError('UNIX_DIAG observation exceeded its bound')
            for kind, flags, seq, body in _messages(data):
                if seq != sequence or flags & _NLM_F_DUMP_INTR:
                    raise ValueError('interrupted or mismatched UNIX_DIAG dump')
                if kind in (2, 3):  # NLMSG_ERROR / NLMSG_DONE
                    if len(body) < 4:
                        raise ValueError('missing netlink completion status')
                    code = struct.unpack_from('=i', body)[0]
                    if code:
                        raise OSError(abs(code), os.strerror(abs(code)))
                    if kind == 3:
                        return result
                    continue
                if kind != _SOCK_DIAG_BY_FAMILY:
                    raise ValueError('unexpected diagnostic message type')
                value = _unix_message(body)
                if selected is None or value['inode'] in selected:
                    if value['inode'] in result:
                        raise ValueError('duplicated socket inode in diagnostic dump')
                    result[value['inode']] = value
