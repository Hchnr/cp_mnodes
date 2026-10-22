#!/usr/bin/env python3
"""Single-master durable queue: no cross-node NFS exclusion primitives."""
import argparse
import collections
import hashlib
import hmac
import http.server
import json
import os
import pathlib
import re
import socket
import sqlite3
import threading
import time
import urllib.parse
import uuid

from migration_io import read_json, write_json


def atomic_json(path, value):
    return write_json(path, value)


def validate_draining_hosts(value):
    if not isinstance(value, list) or len(value) > 256:
        raise ValueError('draining_hosts must be a list of at most 256 hostnames')
    if any(not isinstance(host, str) or
           not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,179}', host) for host in value):
        raise ValueError('draining_hosts contains an invalid hostname')
    return set(value)


class Broker:
    def __init__(self,control):
        self.control=pathlib.Path(control)
        self.cfg=read_json(self.control/'config.json')
        self.lock=threading.Lock()
        try:
            drain_state=read_json(self.control/'drain-hosts.json')
        except FileNotFoundError:
            drain_state={'draining_hosts':[]}
        if not isinstance(drain_state,dict):
            raise ValueError('invalid persisted drain state')
        self.draining_hosts=validate_draining_hosts(drain_state.get('draining_hosts',[]))
        self.db_path='/tmp/'+self.cfg['run_id']+'-broker.sqlite'
        self.db=sqlite3.connect(self.db_path,check_same_thread=False)
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('CREATE TABLE IF NOT EXISTS claims '
                        '(stage TEXT,id INTEGER,owner TEXT,lease TEXT,status TEXT,payload TEXT,running TEXT,'
                        'PRIMARY KEY(stage,id))')
        self.db.execute('CREATE INDEX IF NOT EXISTS claims_owner_live ON claims(stage,owner,status)')
        self.db.execute('CREATE INDEX IF NOT EXISTS claims_status_owner ON claims(status,owner)')
        self.db.commit()
        self.ready={}
        self.next_scan={}
        self.state_cache=None
        self.state_cache_until=0.0

    def claim(self,stage,owner):
        if not re.fullmatch(r'p[123]-r[123]',stage) or not re.fullmatch(r'[A-Za-z0-9_.-]{1,180}',owner):
            raise ValueError('invalid stage or owner')
        with self.lock:
            # A lost HTTP response cannot strand a task or assign another one.
            row=self.db.execute("SELECT id,lease,payload,running FROM claims WHERE stage=? AND owner=? AND status='leased'",
                                (stage,owner)).fetchone()
            if row:
                root=self.control/'stages'/stage
                running=pathlib.Path(row[3])
                if not running.exists():
                    atomic_json(running,json.loads(row[2]))
                pending=root/'pending'/str(row[0]%64)/(str(row[0])+'.json')
                try:pending.unlink()
                except FileNotFoundError:pass
                return {'task':json.loads(row[2]),'lease':row[1]}
            # Outstanding leases above must remain recoverable after a lost
            # response. Draining prevents only fresh work for this host.
            if owner.rsplit('-',1)[0] in self.draining_hosts:
                return {'task':None,'draining':True}
            tick=time.monotonic()
            if self.state_cache is None or tick>=self.state_cache_until:
                self.state_cache=read_json(self.control/'state.json')
                self.state_cache_until=time.monotonic()+0.5
            state=self.state_cache
            if state.get('stage')!=stage or state['status'] in ('failed','stopped','complete','complete_with_changes'):
                return {'task':None}
            root=self.control/'stages'/stage
            ready=self.ready.setdefault(stage,collections.deque())
            if not ready and time.monotonic()>=self.next_scan.get(stage,0):
                for shard in range(64):
                    parent=root/'pending'/str(shard)
                    if parent.is_dir():
                        ready.extend(p for p in parent.iterdir() if p.name.endswith('.json'))
                self.next_scan[stage]=time.monotonic()+0.5
            while ready:
                path=ready.popleft()
                task_id=int(path.stem)
                row=self.db.execute('SELECT status,payload FROM claims WHERE stage=? AND id=?',
                                    (stage,task_id)).fetchone()
                if row:
                    # Pending can remain visible briefly through an NFS cache.
                    continue
                try:
                    task=read_json(path)
                except FileNotFoundError:
                    continue
                if task['id']!=task_id:
                    raise RuntimeError('queue payload id mismatch')
                lease=uuid.uuid4().hex
                running=root/'running'/('broker--'+owner+'--'+str(task_id)+'.json')
                payload=json.dumps(task,ensure_ascii=True)
                # The local transaction is the only authority for assignment.
                self.db.execute('INSERT INTO claims VALUES (?,?,?,?,?,?,?)',
                                (stage,task_id,owner,lease,'leased',payload,str(running)))
                self.db.commit()
                atomic_json(running,task)
                try:
                    path.unlink()
                except FileNotFoundError:
                    pass
                return {'task':task,'lease':lease}
            return {'task':None}

    def update_control(self,data):
        if not isinstance(data,dict) or 'draining_hosts' not in data:
            raise ValueError('control requires draining_hosts')
        hosts=validate_draining_hosts(data['draining_hosts'])
        with self.lock:
            atomic_json(self.control/'drain-hosts.json',
                        {'draining_hosts':sorted(hosts),'updated':time.time()})
            self.draining_hosts=hosts
            return {'ok':True,'draining_hosts':sorted(hosts)}

    def get_control(self):
        # A congested shared-filesystem operation in claim() must not block
        # drain observation. WAL gives this local reader a committed snapshot.
        draining=sorted(self.draining_hosts)
        uri='file:'+urllib.parse.quote(self.db_path,safe='/')+'?mode=ro'
        reader=sqlite3.connect(uri,uri=True,timeout=2.0)
        try:
            leased=collections.Counter()
            for owner,count in reader.execute(
                    "SELECT owner,COUNT(*) FROM claims WHERE status='leased' GROUP BY owner"):
                leased[owner.rsplit('-',1)[0]]+=count
        finally:
            reader.close()
        return {'ok':True,'draining_hosts':draining,
                'leased_by_host':dict(sorted(leased.items()))}

    def done(self,data):
        stage=data['stage']; task_id=int(data['id']); lease=data['lease']
        with self.lock:
            row=self.db.execute('SELECT lease,status,running,payload FROM claims WHERE stage=? AND id=?',
                                (stage,task_id)).fetchone()
            if not row or not hmac.compare_digest(row[0],lease):
                raise ValueError('invalid lease')
            if row[1]=='done':
                return {'ok':True,'already_done':True}
            receipt=data['receipt']
            task=json.loads(row[3])
            if receipt['id']!=task_id or [r['rel'] for r in receipt['results']] != [e['rel'] for e in task['entries']]:
                raise ValueError('receipt does not cover assigned entries')
            root=self.control/'stages'/stage
            target=root/'done'/str(task_id%64)/(str(task_id)+'.json')
            # A first completion normally has no receipt yet. read_json()
            # retries missing publications for 60ms, which would serialize
            # every new completion behind that sleep while holding the lock.
            # This broker is the sole receipt publisher, so check both the
            # canonical and versioned paths before entering that retry path.
            snapshots=target.parent/'.snapshots'/target.name
            if target.exists() or snapshots.exists():
                try:
                    old=read_json(target)
                    prior={r['rel']:r for r in old['results']}
                except FileNotFoundError:
                    prior={}
            else:
                prior={}
            # Preserve proof that a replayed path was originally created by us.
            # Equal-content skips have been re-hashed by the worker.
            for result in receipt['results']:
                previous=prior.get(result['rel'])
                if result['status']=='skipped' and previous and previous['status']=='copied':
                    result['status']='copied'
                    result['bytes']=previous.get('bytes',0)
                    result['replay_reused_copy']=True
            atomic_json(target,receipt)
            try:
                pathlib.Path(row[2]).unlink()
            except FileNotFoundError:
                pass
            self.db.execute("UPDATE claims SET status='done' WHERE stage=? AND id=?",(stage,task_id))
            self.db.commit()
            return {'ok':True}


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--control',required=True)
    parser.add_argument('--port',type=int,default=37651)
    args=parser.parse_args()
    os.umask(0o077)
    import fcntl
    control=pathlib.Path(args.control)
    cfg=read_json(control/'config.json')
    lock=open('/tmp/'+cfg['run_id']+'-broker.lock','a')
    fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    broker=Broker(control)

    class Handler(http.server.BaseHTTPRequestHandler):
        protocol_version='HTTP/1.1'

        def reply(self,status,value):
            payload=json.dumps(value,ensure_ascii=True).encode()
            self.send_response(status)
            self.send_header('Content-Type','application/json')
            self.send_header('Content-Length',str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def authorized(self):
            return hmac.compare_digest(self.headers.get('Authorization',''),'Bearer '+cfg['broker_token'])

        def do_GET(self):
            if not self.authorized():
                self.reply(403,{'error':'unauthorized'});return
            url=urllib.parse.urlparse(self.path)
            try:
                if url.path=='/health':
                    self.reply(200,{'ok':True,'pid':os.getpid()});return
                if url.path=='/control':
                    self.reply(200,broker.get_control());return
                if url.path!='/claim':
                    self.reply(404,{'error':'not found'});return
                query=urllib.parse.parse_qs(url.query)
                self.reply(200,broker.claim(query['stage'][0],query['owner'][0]))
            except Exception as exc:
                self.reply(500,{'error':type(exc).__name__+': '+str(exc)})

        def do_POST(self):
            if not self.authorized():
                self.reply(403,{'error':'unauthorized'});return
            if self.path not in ('/done','/control'):
                self.reply(404,{'error':'not found'});return
            try:
                length=int(self.headers.get('Content-Length',0))
                if not 0<length<64*1024**2:
                    raise ValueError('invalid body length')
                data=json.loads(self.rfile.read(length))
                self.reply(200,broker.done(data) if self.path=='/done' else broker.update_control(data))
            except Exception as exc:
                self.reply(500,{'error':type(exc).__name__+': '+str(exc)})

        def log_message(self,*args):
            pass

    class BrokerHTTPServer(http.server.ThreadingHTTPServer):
        request_queue_size=1024

    server=BrokerHTTPServer(('0.0.0.0',args.port),Handler)
    atomic_json(control/'broker.json',{'pid':os.getpid(),'port':args.port,'host':socket.gethostname(),
                                      'started':time.time()})
    server.serve_forever(poll_interval=0.5)


if __name__=='__main__':
    main()
