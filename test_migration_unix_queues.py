import errno
import os
import socket
import struct
import sys
import unittest

from migration_unix_queues import unix_queues, _messages, _unix_message


class ProtocolTests(unittest.TestCase):
    def test_malformed_netlink_and_unix_attributes_are_not_treated_as_empty_queues(self):
        for data in (b'bad', struct.pack('=IHHII', 0, 20, 0, 1, 0)):
            with self.assertRaises(ValueError):
                list(_messages(data))
        header = struct.pack('=BBBBIII', socket.AF_UNIX, 1, 1, 0, 42, 0, 0)
        for data in (b'', header, header+struct.pack('=HHI', 8, 4, 1)):
            with self.assertRaises(ValueError):
                _unix_message(data)

    @unittest.skipUnless(sys.platform == 'linux', 'Linux socket diagnostics')
    def test_native_socketpair_queues_are_observed_without_consuming_messages(self):
        left, right = socket.socketpair()
        with left, right:
            a, b = os.fstat(left.fileno()).st_ino, os.fstat(right.fileno()).st_ino
            try:
                before = unix_queues({a, b})
            except OSError as exc:
                if exc.errno in (errno.EPERM, errno.EACCES, errno.EPROTONOSUPPORT,
                                 errno.EAFNOSUPPORT):
                    self.skipTest('socket diagnostics are unavailable: '+str(exc))
                raise
            self.assertEqual(before[a]['peer'], b)
            self.assertEqual(before[b]['peer'], a)
            self.assertEqual(before[b]['receive_bytes'], 0)
            left.sendall(b'private-fixture-payload')
            queued = unix_queues({a, b})
            self.assertGreaterEqual(queued[b]['receive_bytes'], len(b'private-fixture-payload'))
            self.assertGreater(queued[a]['send_bytes'], 0)
            self.assertEqual(right.recv(128), b'private-fixture-payload')
            after = unix_queues({a, b})
            self.assertEqual(after[b]['receive_bytes'], 0)
            self.assertEqual(after[a]['send_bytes'], 0)


if __name__ == '__main__':
    unittest.main()
