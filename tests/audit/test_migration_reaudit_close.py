import json
import gzip
import os
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest import mock

import migration_reaudit_close as close
from migration_reaudit import atomic


def row(rel,size=5):
    source=dict(ino=1,mode=33188,size=size,mtime_ns=1,ctime_ns=1,uid=0,gid=0)
    obs=dict(rel=rel,kind='file',source=source,reason='target_absent')
    value=dict(rel=rel,kind='file',source=source,observation=obs,status='missing',checked_at=1)
    return (os.fsencode(rel),'missing','file',size,json.dumps(value),json.dumps(obs),1,0)


class CloseTests(unittest.TestCase):
    def test_resumed_capture_requires_exact_union_not_just_larger_count(self):
        with tempfile.TemporaryDirectory() as directory:
            folder=Path(directory);report=folder/'reviews/slot-0/report';report.mkdir(parents=True)
            cache=folder/'overlay.sqlite';db=sqlite3.connect(cache);db.executescript(close.SCHEMA)
            db.execute('CREATE TABLE candidates(rel BLOB PRIMARY KEY,parent BLOB,raw TEXT)')
            a=row('a');b=row('b')
            def output(path,value):
                with gzip.open(path,'wt') as f:f.write(value[4]+'\n')
            original=report/'observations.jsonl.gz';output(original,a)
            task=folder/'tasks.gz'
            with gzip.open(task,'wt') as f:f.write(json.dumps(dict(parent='',rows=[dict(rel='a')]))+'\n')
            plan=dict(slot=0,records=1,input=str(task),input_sha256=close.digest(task))
            result=dict(status='complete',input_sha256=plan['input_sha256'],output=str(original),
                        output_sha256=close.digest(original),counts=close.counters([json.loads(a[4])]))
            atomic(report/'result.json',result);atomic(report.parent/'launch.json',dict(pid=1,start='not-real'))
            close.ingest(db,plan,result)
            capture=folder/'stable-captures/token';capture.mkdir(parents=True)
            proof=capture/'namespace-proof.json';atomic(proof,dict(scope='b'))
            extra=capture/'observations.jsonl.gz';output(extra,b)
            atomic(capture/'result.json',dict(status='complete',scope='b',proof=str(proof),
                input_sha256=close.digest(proof),output=str(extra),output_sha256=close.digest(extra),
                counts=close.counters([json.loads(b[4])])))
            db.execute('INSERT INTO reviewed VALUES(?,?,?,?,?,?,?,?)',b)
            for value in (a,b):db.execute('INSERT INTO candidates VALUES(?,?,?)',(value[0],b'',value[4]))
            db.commit();db.close()
            atomic(folder/'review-manifest.json',dict(cache=str(cache),records=1,plans=[plan]))
            with mock.patch.object(close,'terminal',return_value=True):
                checked,_=close.load_reviews(folder,folder);checked.close()
                db=sqlite3.connect(cache);c=row('c');db.execute('INSERT INTO reviewed VALUES(?,?,?,?,?,?,?,?)',c)
                db.execute('INSERT INTO candidates VALUES(?,?,?)',(c[0],b'',c[4]));db.commit();db.close()
                with self.assertRaises(ValueError):close.load_reviews(folder,folder)

    def test_effective_scope_replaces_stale_missing_and_includes_new_paths(self):
        db=sqlite3.connect(':memory:');self.addCleanup(db.close);db.executescript(close.SCHEMA)
        db.execute("ATTACH DATABASE ':memory:' AS overlay")
        db.execute('CREATE TABLE overlay.reviewed AS SELECT * FROM main.reviewed')
        db.executemany('INSERT INTO main.reviewed VALUES(?,?,?,?,?,?,?,?)',[row('a'),row('b')])
        gone=list(row('a'));gone[1]='source_disappeared';gone[5]=None
        db.executemany('INSERT INTO overlay.reviewed VALUES(?,?,?,?,?,?,?,?)',[gone,row('c',9)])
        self.assertEqual([(r[0],r[1]) for r in close.effective_rows(db,'file')],[(b'b',5),(b'c',9)])

    def test_scoped_scan_exception_must_be_resolved(self):
        with tempfile.TemporaryDirectory() as d:
            path=Path(d)/'errors.jsonl';path.write_text(json.dumps(dict(rel='a',operation='source_directory_enumeration'))+'\n')
            db=sqlite3.connect(':memory:');self.addCleanup(db.close);db.executescript(close.SCHEMA)
            manifest=dict(scans=[dict(inputs=[dict(category='errors',path=str(path),sha256=close.digest(path))])])
            with self.assertRaises(ValueError):close.pending_scopes(db,manifest)
            r=list(row('a'));r[1]='source_disappeared';db.execute('INSERT INTO reviewed VALUES(?,?,?,?,?,?,?,?)',r)
            self.assertEqual(close.pending_scopes(db,manifest),[])

    def test_final_export_binds_exact_scope_and_only_authorizes_below_threshold(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);folder=root/'final/namespace';folder.mkdir(parents=True);(root/'tools').mkdir()
            main_path=root/'main.sqlite';main=sqlite3.connect(main_path);main.executescript(close.SCHEMA)
            main.execute('INSERT INTO reviewed VALUES(?,?,?,?,?,?,?,?)',row('a',9));main.commit();main.close()
            overlay_path=root/'overlay.sqlite';db=sqlite3.connect(overlay_path);self.addCleanup(db.close);db.executescript(close.SCHEMA)
            original=dict(coverage=dict(missing_reviews=0,unexpected_paths=0),copy_gates=dict(unresolved_access_paths=0))
            atomic(root/'final/reviewed-manifest.json',original)
            atomic(root/'plan.json',dict(units=[dict(rel='',leaves_only=False)],planning_errors=[]))
            atomic(root/'source-sha256.json',{})
            atomic(root/'preflight.json',dict(nodes=[dict(source_ino=11,target_ino=22)]))
            atomic(root/'conditional-copy-policy.json',dict(conditional_copy_authorized=True,threshold_bytes_exclusive=close.THRESHOLD))
            realconnect=sqlite3.connect;realstat=os.stat
            def connect(path,*a,**kw):
                if str(path).startswith('file:/tmp/cp-mnodes-reaudit-index-'):return realconnect(main_path)
                return realconnect(path,*a,**kw)
            def stat(path,*a,**kw):
                if path in ('/share/project','/share-mixed/project'):
                    return mock.Mock(st_ino=11 if path=='/share/project' else 22)
                return realstat(path,*a,**kw)
            with mock.patch.object(close.sqlite3,'connect',side_effect=connect),mock.patch.object(close.os,'stat',side_effect=stat),\
                    mock.patch.object(close,'snapshot',return_value=dict(enumeration_complete=True)),mock.patch.object(close,'closed_scans'):
                result=close.export(root,folder,db,dict(cache=str(overlay_path)),[])
            auth=json.loads((root/'final/conditional-copy-authorization.json').read_text())
            self.assertEqual(result['missing']['file'],dict(paths=1,logical_bytes=9))
            self.assertEqual(auth['final_report_sha256'],close.digest(root/'final/final-report.json'))
            self.assertEqual(auth['ordinary_files'],result['missing']['file'])
            self.assertFalse(result['content_hashes_verified'])
            self.assertFalse(result['data_copy_started'])


if __name__=='__main__':unittest.main()
