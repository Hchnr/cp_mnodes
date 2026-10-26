import gzip
import json
from pathlib import Path
import tempfile
import unittest

from migration_reaudit_provenance import compare


class ProvenanceTests(unittest.TestCase):
    def test_overlap_and_timestamps_are_separate_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            def write(name,rows):
                path=root/name
                with gzip.open(path,'wt') as f:
                    for row in rows:f.write(json.dumps(row)+'\n')
                return path
            def row(rel,t,size):return dict(rel=rel,source=dict(size=size,ctime_ns=t,mtime_ns=t))
            old=write('old.gz',[row('a',1,1),row('b',1,2)])
            new=write('new.gz',[row('b',1,2),row('c',2000000000,3)])
            result=compare(old,new,1,root/'out.json')
            self.assertEqual(result['counts']['in_previous_missing_manifest_files'],1)
            self.assertEqual(result['counts']['not_in_previous_missing_manifest_bytes'],3)
            self.assertEqual(result['counts']['ctime_and_mtime_not_after_previous_audit_files'],1)
            self.assertIn('not creation time',result['caveat'])


if __name__=='__main__':unittest.main()
