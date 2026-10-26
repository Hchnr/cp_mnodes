import unittest
from finalize_priority4_source_exit import terminal


class TerminalExitTests(unittest.TestCase):
    def test_empty_argv_is_accepted_only_after_exact_terminal_identity(self):
        expected = dict(pid=123, start_ticks='456', argv=['old'])
        terminal(dict(expected, state='Z', argv=[]), expected)
        terminal(None, expected)
        for change in (dict(state='R', argv=[]), dict(state='D', argv=[]),
                       dict(state='Z', start_ticks='reused'), dict(state='Z', pid=124)):
            with self.assertRaises(RuntimeError):
                terminal(dict(expected, **change), expected)


if __name__ == '__main__':
    unittest.main()
