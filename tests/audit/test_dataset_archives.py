"""Private fixtures only: no shared datasets are read by these tests."""
import importlib.util
from pathlib import Path
import tempfile
import unittest
from unittest import mock

SOURCE = Path(__file__).resolve().parents[2]/'ops/analysis/dataset_archives.py'
if not SOURCE.is_file():
    SOURCE = Path(__file__).with_name('dataset_archives.py')
spec = importlib.util.spec_from_file_location('archive_review', SOURCE)
review = importlib.util.module_from_spec(spec)
spec.loader.exec_module(review)


class ArchiveReviewTests(unittest.TestCase):
    def test_multipart_and_plain_archive_names(self):
        for name in ('Grasping.tar.gz.03', 'task.tar.gz.part-aa',
                     'task.tar.gz.aa', 'dataset.zip', 'dataset.7z.001', 'train_0000.tar'):
            self.assertIsNotNone(review.archive_name(name), name)

    def test_nonarchives_and_temporary_files_are_excluded(self):
        for name in ('episode.hdf5', 'data.parquet', 'video.mp4',
                     'dataset.zip.tmp', 'task.tar.gz.log', 'data.json.gz'):
            self.assertIsNone(review.archive_name(name), name)

    def test_metadata_listing_and_symlinks_use_private_fixture_only(self):
        with tempfile.TemporaryDirectory(prefix='archive-review-private-') as temp:
            root = Path(temp)/'source'
            root.mkdir()
            (root/'dataset.zip').write_bytes(b'private dummy archive, never opened')
            (root/'episode.hdf5').write_bytes(b'private fixture')
            (root/'dataset-link.zip').symlink_to(root/'dataset.zip')
            with mock.patch.object(review, 'P', str(root)), mock.patch.object(review, 'S', str(root)):
                result = review.listing(str(root))
            self.assertTrue(result['complete'])
            self.assertEqual([x['name'] for x in result['archives']], ['dataset.zip'])
            self.assertEqual(result['archives'][0]['bytes'], (root/'dataset.zip').stat().st_size)
            self.assertEqual(result['regular_nonarchives'], 1)

    def test_symlinked_directory_component_is_not_followed(self):
        with tempfile.TemporaryDirectory(prefix='archive-review-private-') as temp:
            root = Path(temp)/'source'
            root.mkdir()
            outside = Path(temp)/'outside'
            outside.mkdir()
            (outside/'keep.zip').write_bytes(b'keep')
            (root/'escape').symlink_to(outside, target_is_directory=True)
            with mock.patch.object(review, 'P', str(root)), mock.patch.object(review, 'S', str(root)):
                result = review.listing(str(root/'escape'))
            self.assertEqual(result['archives'], [])
            self.assertFalse(result['complete'])
            self.assertTrue(result['errors'])
            self.assertTrue((outside/'keep.zip').exists())

    def test_partial_listing_is_explicit(self):
        with tempfile.TemporaryDirectory(prefix='archive-review-private-') as temp:
            root = Path(temp)
            for i in range(3):
                (root/f'{i}.zip').touch()
            with mock.patch.object(review, 'P', str(root)), mock.patch.object(review, 'S', str(root)):
                result = review.listing(str(root), 2)
            self.assertFalse(result['complete'])
            self.assertEqual(len(result['archives']), 2)

    def test_candidate_requires_both_receipts_and_live_output(self):
        source = dict(archives=[dict(bytes=12)], complete=True, errors=[])
        previous = dict(status='PASS', compressed_total=12)
        verified = dict(status='PASS')
        dest = dict(state='nonempty')
        self.assertTrue(review.verified_candidate(source, previous, verified, dest))
        for changes in (dict(status='pending'), dict(compressed_total=13)):
            bad = dict(previous, **changes)
            self.assertFalse(review.verified_candidate(source, bad, verified, dest))
        self.assertFalse(review.verified_candidate(source, previous, dict(status='pending'), dest))
        self.assertFalse(review.verified_candidate(source, previous, verified, dict(state='absent')))
        for change in (dict(complete=False), dict(errors=['read error']), dict(archives=[])):
            self.assertFalse(review.verified_candidate(dict(source, **change), previous, verified, dest))


if __name__ == '__main__':
    unittest.main()
