#!/usr/bin/env python3
"""Authenticated, bounded, read-only target metadata probes on one copy node."""
import argparse
import collections
import concurrent.futures
import datetime
import fcntl
import hmac
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import resource
import socket
import stat
import sys
import threading
import time
from urllib.parse import urlsplit

from migration_io import read_json, write_json
from migration_scan import _stat_fields
from migration_worker import _components, _open_parent

SOURCE = '/share/project/eai_pwm'
DESTINATION = '/share-mixed/project/eai_pwm'
MAX_BODY = 128 * 1024
MAX_BATCH = 64


class ProbeError(Exception):
    def __init__(self, status, reason):
        self.status, self.reason = status, reason
        super().__init__(reason)


def phase3_relative(entry):
    if not isinstance(entry, dict) or not isinstance(entry.get('rel'), str):
        raise ProbeError(400, 'entry must contain a relative path')
    rel = entry['rel']
    try:
        _components(rel)
    except (ValueError, OSError) as exc:
        raise ProbeError(400, 'unsafe relative path') from exc
    if (rel == 'home/hcr' or rel.startswith('home/hcr/') or
            rel == 'repos/cosmos-framework-pwm' or rel.startswith('repos/cosmos-framework-pwm/')):
        raise ProbeError(400, 'path is outside priority3')
    if entry.get('kind', 'file') not in ('file', 'symlink'):
        raise ProbeError(400, 'only regular files and symlinks may be probed')
    return rel


class ProbeService:
    def __init__(self, control, cfg, slots=32, stop_cache_seconds=0):
        self.control = Path(control).resolve()
        if (cfg.get('source'), cfg.get('destination')) != (SOURCE, DESTINATION):
            raise ValueError('metadata probes are restricted to eai_pwm')
        if not isinstance(cfg.get('broker_token'), str) or not cfg['broker_token']:
            raise ValueError('private authentication token is required')
        if type(slots) is not int or not 1 <= slots <= 128:
            raise ValueError('metadata concurrency must be within1..128')
        if not 0 <= stop_cache_seconds <= .1:
            raise ValueError('STOP cache must be within 0..0.1 seconds')
        self.stop_cache_seconds = stop_cache_seconds
        self._stop_until = 0
        self._stop_seen = False
        self._stop_mutex = threading.Lock()
        self.cfg = cfg
        self.source_root, self.destination_root = cfg['source'], cfg['destination']
        self.slots = slots
        self.fs_slots = threading.BoundedSemaphore(slots)
        self.mutex = threading.Lock()
        self.counts = collections.Counter()
        self.inflight = 0
        self.peak_inflight = 0
        self.started = datetime.datetime.now(datetime.timezone.utc).isoformat()
        self.identity = {}
        # Batch requests share the same bounded workers for the service's
        # lifetime. A batch neither creates a new pool nor bypasses fs_slots.
        self.batch_pool = concurrent.futures.ThreadPoolExecutor(
            max_workers=slots, thread_name_prefix='probe-metadata')

    def close(self):
        self.batch_pool.shutdown(wait=True, cancel_futures=False)

    def authorized(self, header):
        # Tokens are never included in replies, role records, or request logs.
        supplied = header[7:] if isinstance(header, str) and header.startswith('Bearer ') else ''
        return hmac.compare_digest(supplied.encode('utf-8'), self.cfg['broker_token'].encode('utf-8'))

    def stopped(self):
        if not self.stop_cache_seconds:
            return (self.control / 'STOP').exists()
        if self._stop_seen:
            return True
        if time.monotonic() >= self._stop_until:
            with self._stop_mutex:
                if time.monotonic() >= self._stop_until:
                    self._stop_seen = (self.control / 'STOP').exists()
                    self._stop_until = time.monotonic() + self.stop_cache_seconds
        return self._stop_seen

    def current_source(self, original):
        rel = phase3_relative(original)
        parent = None
        try:
            parent, name = _open_parent(self.source_root, rel)
            info = os.stat(name, dir_fd=parent, follow_symlinks=False)
            fresh = {'rel': rel, **_stat_fields(info)}
            if stat.S_ISREG(info.st_mode):
                fresh['kind'] = 'file'
            elif stat.S_ISLNK(info.st_mode):
                target = os.readlink(name, dir_fd=parent)
                after = os.stat(name, dir_fd=parent, follow_symlinks=False)
                if (after.st_dev, after.st_ino, after.st_ctime_ns) != (info.st_dev, info.st_ino, info.st_ctime_ns):
                    raise ProbeError(409, 'source symlink changed during observation')
                fresh.update(kind='symlink', target=target, link_target=target)
            else:
                raise ProbeError(409, 'source is no longer a regular file or symlink')
            if isinstance(original.get('known'), dict):
                fresh['known'] = original['known']
            return fresh
        except OSError as exc:
            raise ProbeError(409, 'source cannot be safely refreshed') from exc
        finally:
            if parent is not None:
                os.close(parent)

    def probe(self, entry):
        phase3_relative(entry)
        if not self.fs_slots.acquire(blocking=False):
            with self.mutex:
                self.counts['busy'] += 1
            raise ProbeError(503, 'metadata slots busy')
        began = time.monotonic()
        parent = None
        with self.mutex:
            self.counts['requests'] += 1
            self.inflight += 1
            self.peak_inflight = max(self.peak_inflight, self.inflight)
        try:
            if self.stopped():
                raise ProbeError(503, 'STOP requested')
            try:
                parent, name = _open_parent(self.destination_root, entry['rel'])
                os.stat(name, dir_fd=parent, follow_symlinks=False)
                exists = True
            except FileNotFoundError:
                exists = False
            except OSError as exc:
                raise ProbeError(409, 'target parent cannot be safely inspected') from exc
            if self.stopped():
                raise ProbeError(503, 'STOP requested')
            fresh = None if exists else (self.current_source(entry) if entry.get('_refresh_source') else dict(entry))
            if isinstance(fresh, dict):
                fresh.pop('_refresh_source', None)
            if self.stopped():
                raise ProbeError(503, 'STOP requested')
            with self.mutex:
                self.counts['existing' if exists else 'missing'] += 1
            return {'ok': True, 'missing': not exists, 'entry': fresh,
                    'reason': 'existing' if exists else 'missing'}
        except ProbeError:
            with self.mutex:
                self.counts['errors'] += 1
            raise
        finally:
            if parent is not None:
                os.close(parent)
            with self.mutex:
                self.inflight -= 1
                self.counts['elapsed_milliseconds'] += int((time.monotonic() - began) * 1000)
            self.fs_slots.release()

    def _batch_one(self, entry):
        try:
            return {'status': 200, 'result': self.probe(entry)}
        except ProbeError as exc:
            return {'status': exc.status, 'reason': exc.reason}
        except OSError:
            return {'status': 503, 'reason': 'metadata observation unavailable'}

    def probe_batch(self, entries):
        # Reject the whole request before any filesystem observation, so an
        # invalid trailing entry cannot cause a partially accepted batch.
        if not isinstance(entries, list) or not 1 <= len(entries) <= MAX_BATCH:
            raise ProbeError(400, 'entries must contain between1 and64 objects')
        for entry in entries:
            phase3_relative(entry)
        with self.mutex:
            self.counts['batch_requests'] += 1
            self.counts['batch_entries'] += len(entries)
        try:
            # map preserves input ordering while metadata runs concurrently.
            results = list(self.batch_pool.map(self._batch_one, entries))
        except RuntimeError:
            raise ProbeError(503, 'metadata service is stopping') from None
        with self.mutex:
            self.counts['batch_failed_entries'] += sum(
                result['status'] != 200 for result in results)
        return {'ok': True, 'results': results}

    def health(self):
        with self.mutex:
            return {'ok': True, **self.identity, 'started': self.started, 'slots': self.slots,
                    'stop_cache_seconds': self.stop_cache_seconds,
                    'batch_limit': MAX_BATCH,
                    'inflight': self.inflight, 'peak_inflight': self.peak_inflight,
                    'counts': dict(self.counts)}


class ProbeHandler(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.0'
    server_version = 'CopyMetadataProbe/1'

    def log_message(self, *_args):
        return  # Never log Authorization or user paths.

    def reply(self, status, value):
        body = json.dumps(value, ensure_ascii=True).encode('utf-8')
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass
        self.close_connection = True

    def authenticated(self):
        if not self.server.service.authorized(self.headers.get('Authorization')):
            self.reply(401, {'ok': False, 'reason': 'authentication required'})
            return False
        return True

    def do_GET(self):
        if not self.authenticated():
            return
        if urlsplit(self.path).path != '/health':
            self.reply(404, {'ok': False, 'reason': 'unknown route'})
            return
        self.reply(200, self.server.service.health())

    def do_POST(self):
        if not self.authenticated():
            return
        route = urlsplit(self.path).path
        if route not in ('/probe', '/probe-batch'):
            self.reply(404, {'ok': False, 'reason': 'unknown route'})
            return
        if self.headers.get('Transfer-Encoding'):
            self.reply(400, {'ok': False, 'reason': 'chunked bodies are not accepted'})
            return
        try:
            length = int(self.headers.get('Content-Length', ''))
        except ValueError:
            self.reply(411, {'ok': False, 'reason': 'Content-Length required'})
            return
        if not 0 < length <= MAX_BODY:
            self.reply(413, {'ok': False, 'reason': 'body exceeds128KiB or is empty'})
            return
        try:
            raw = self.rfile.read(length)
            if len(raw) != length:
                raise ValueError('incomplete body')
            body = json.loads(raw)
            field = 'entries' if route == '/probe-batch' else 'entry'
            if not isinstance(body, dict) or field not in body:
                raise ValueError('probe entries required')
            response = (self.server.service.probe_batch(body[field])
                        if route == '/probe-batch' else self.server.service.probe(body[field]))
        except ProbeError as exc:
            self.reply(exc.status, {'ok': False, 'reason': exc.reason})
            return
        except (ValueError, UnicodeError, TypeError):
            self.reply(400, {'ok': False, 'reason': 'invalid JSON body'})
            return
        except OSError:
            self.reply(503, {'ok': False, 'reason': 'metadata observation unavailable'})
            return
        self.reply(200, response)


class BoundedProbeServer(ThreadingHTTPServer):
    daemon_threads = True
    block_on_close = False
    request_queue_size = 128

    def __init__(self, address, service):
        self.service = service
        self.http_slots = threading.BoundedSemaphore(service.slots * 2)
        self.http_inflight = 0
        super().__init__(address, ProbeHandler)
        self.timeout = .5

    def get_request(self):
        request, address = super().get_request()
        request.settimeout(10)
        return request, address

    def process_request(self, request, client_address):
        if not self.http_slots.acquire(blocking=False):
            try:
                request.sendall(b'HTTP/1.0 503 Busy\r\nContent-Length: 0\r\n\r\n')
            except OSError:
                pass
            self.shutdown_request(request)
            return
        try:
            super().process_request(request, client_address)
        except BaseException:
            self.http_slots.release()
            raise

    def process_request_thread(self, request, client_address):
        try:
            super().process_request_thread(request, client_address)
        finally:
            self.http_slots.release()

    def handle_error(self, request, client_address):
        # A client timeout/disconnect never dumps headers, bodies, or secrets.
        with self.service.mutex:
            self.service.counts['connection_errors'] += 1

    def server_close(self):
        try:
            super().server_close()
        finally:
            self.service.close()


def identity():
    fields = (Path('/proc') / str(os.getpid()) / 'stat').read_text().rsplit(') ', 1)[1].split()
    return {'pid': os.getpid(), 'starttime_ticks': fields[19],
            'argv': (Path('/proc') / str(os.getpid()) / 'cmdline').read_bytes().rstrip(b'\0').decode().split('\0')}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--control', required=True)
    parser.add_argument('--port', type=int, default=0)
    parser.add_argument('--slots', type=int, default=32)
    parser.add_argument('--slots32', dest='slots', action='store_const', const=32)
    parser.add_argument('--stop-cache-seconds', type=float, default=0)
    parser.add_argument('--record', required=True)
    parser.add_argument('--bind', default='0.0.0.0')
    parser.add_argument('--advertise-address')
    args = parser.parse_args()
    control = Path(args.control).resolve()
    record_path = Path(args.record).resolve()
    if os.path.commonpath((str(control), str(record_path))) != str(control):
        parser.error('role record must remain within the control directory')
    os.umask(0o077)
    cfg = read_json(control / 'config.json')
    service = ProbeService(control, cfg, args.slots, args.stop_cache_seconds)
    if service.stopped():
        raise RuntimeError('STOP requested; probe service will not start')
    lock = open('/tmp/' + cfg['run_id'] + '-targetprobe.lock', 'a')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    if hard != resource.RLIM_INFINITY and hard < 65536:
        raise RuntimeError('NOFILE hard limit is below65536')
    if soft < 65536:
        resource.setrlimit(resource.RLIMIT_NOFILE, (65536, hard))
    server = BoundedProbeServer((args.bind, args.port), service)
    host = socket.gethostname()
    advertised = args.advertise_address or socket.gethostbyname(host)
    endpoint = 'http://' + advertised + ':' + str(server.server_address[1])
    service.identity = {**identity(), 'host': host, 'endpoint': endpoint,
                        'url': endpoint, 'port': server.server_address[1],
                        'run_id': cfg['run_id'], 'control': str(control),
                        'source': cfg['source'], 'destination': cfg['destination']}
    role = service.health()
    role.update(status='ready', updated=datetime.datetime.now(datetime.timezone.utc).isoformat())
    write_json(record_path, role)
    print('READY ' + json.dumps(role), flush=True)
    stopped = threading.Event()
    def heartbeat():
        while not stopped.wait(5):
            value = service.health()
            value.update(status='stopping' if service.stopped() else 'ready',
                         updated=datetime.datetime.now(datetime.timezone.utc).isoformat())
            try:
                write_json(record_path, value)
            except (OSError, ValueError):
                # Health stays available; status publication cannot block HTTP.
                with service.mutex:
                    service.counts['heartbeat_errors'] += 1
    reporter = threading.Thread(target=heartbeat, name='probe-health', daemon=True)
    reporter.start()
    try:
        while not service.stopped():
            server.handle_request()
    finally:
        stopped.set()
        server.server_close()
        value = service.health()
        value.update(status='stopped', ended=datetime.datetime.now(datetime.timezone.utc).isoformat())
        write_json(record_path, value)
        lock.close()


if __name__ == '__main__':
    main()
