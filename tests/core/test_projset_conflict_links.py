import importlib.util
import os
from pathlib import Path
import sys
import tempfile
import unittest


REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / 'src/core'))
import migration_linux_open as native

spec = importlib.util.spec_from_file_location(
    'projset_conflict_links', REPO / 'ops/diagnostics/projset_conflict_links.py')
links = importlib.util.module_from_spec(spec)
spec.loader.exec_module(links)


class ConflictLinkTests(unittest.TestCase):
    def test_same_object_and_corresponding_paths_are_distinct_results(self):
        with tempfile.TemporaryDirectory() as tmp:
            src, dst = Path(tmp) / 'source', Path(tmp) / 'target'
            src.mkdir(); dst.mkdir()
            (src / 'data').write_text('source'); (dst / 'data').write_text('different')
            (src / 'same').symlink_to(dst / 'data'); (dst / 'same').symlink_to('data')
            (src / 'mapped').symlink_to('data'); (dst / 'mapped').symlink_to('data')
            roots = (str(src), str(dst))
            a = links.resolve(native, roots, roots[0], 'same')
            b = links.resolve(native, roots, roots[1], 'same')
            self.assertEqual(links.compare(a, b, *roots), 'same_current_object')
            a = links.resolve(native, roots, roots[0], 'mapped')
            b = links.resolve(native, roots, roots[1], 'mapped')
            self.assertEqual(links.compare(a, b, *roots),
                             'corresponding_relative_paths_content_unverified')

    def test_symlink_expansion_precedes_dotdot_and_preserves_parent(self):
        with tempfile.TemporaryDirectory() as tmp:
            src, dst = Path(tmp) / 'source', Path(tmp) / 'target'
            src.mkdir(); dst.mkdir()
            (src / 'actual/nested').mkdir(parents=True)
            (src / 'actual/file').write_text('payload')
            (src / 'inner').symlink_to('actual/nested')
            (src / 'link').symlink_to('inner/../file')
            (src / 'actual/nested/up').symlink_to('..')
            roots = (str(src), str(dst))
            result = links.resolve(native, roots, roots[0], 'link')
            self.assertEqual(result['path'], str(src / 'actual/file'))
            parent = links.resolve(native, roots, roots[0], 'actual/nested/up')
            self.assertEqual(parent['path'], str(src / 'actual'))
            self.assertEqual(parent['ino'], (src / 'actual').stat().st_ino)

    def test_scope_escape_and_link_loop_fail_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            src, dst = Path(tmp) / 'source', Path(tmp) / 'target'
            src.mkdir(); dst.mkdir()
            (src / 'relative_escape').symlink_to('../secret')
            (src / 'absolute_escape').symlink_to('/etc/passwd')
            (src / 'a').symlink_to('b'); (src / 'b').symlink_to('a')
            roots = (str(src), str(dst))
            for name in ('relative_escape', 'absolute_escape'):
                with self.subTest(name=name), self.assertRaises(links.OutsideScope):
                    links.resolve(native, roots, roots[0], name)
            with self.assertRaisesRegex(ValueError, 'symlink limit'):
                links.resolve(native, roots, roots[0], 'a', max_links=3)

    def test_unresolved_or_changed_targets_are_never_marked_equal(self):
        self.assertEqual(links.compare({'error': 'OSError'}, {}, '/s', '/t'), 'unresolved')
        a = {'path': '/t/x', 'dev': 1, 'ino': 1}
        b = {'path': '/t/x', 'dev': 1, 'ino': 2}
        self.assertEqual(links.compare(a, b, '/s', '/t'), 'changed_during_comparison')


if __name__ == '__main__':
    unittest.main()
