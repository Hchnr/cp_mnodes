#!/usr/bin/env python3
"""Read-only distributed GNU du inventory with streaming progress and depth-5 reports."""
import argparse
import collections
import concurrent.futures
import csv
import datetime
import hashlib
import json
import os
from pathlib import Path
import queue
import shlex
import signal
import socket
import stat
import subprocess
import sys
import threading
import time
import uuid

REPO = Path(__file__).resolve().parents[2]
SOURCE = '/share-mixed/projset'
POINTER = REPO/'runs/projset-usage-current.json'
FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC


def stamp():
    return datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=8))).isoformat()


def write_json(path, value):
    path = Path(path)
    temp = path.with_name(path.name+'.'+uuid.uuid4().hex+'.tmp')
    temp.write_text(json.dumps(value, ensure_ascii=True, indent=2)+'\n')
    os.replace(temp, path)


def read_json(path):
    return json.loads(Path(path).read_text())


def components(rel):
    if rel == '':
        return []
    if not isinstance(rel, str) or rel.startswith('/') or '\x00' in rel:
        raise ValueError('unsafe relative path')
    parts = rel.split('/')
    if any(p in ('', '.', '..') for p in parts):
        raise ValueError('unsafe component')
    return parts


def directory(source, rel=''):
    fd = os.open('/', FLAGS)
    try:
        for part in [x for x in source.split('/') if x]+components(rel):
            child = os.open(part, FLAGS, dir_fd=fd)
            os.close(fd)
            fd = child
        return fd
    except BaseException:
        os.close(fd)
        raise


def ancestors(rel):
    while rel:
        rel = rel.rpartition('/')[0]
        yield rel


def identity(source):
    fd = directory(source)
    try:
        inode = os.fstat(fd).st_ino
    finally:
        os.close(fd)
    mounts = Path('/proc/self/mountinfo').read_text().splitlines()
    submounts = [x for x in mounts if x.split()[4].startswith(source+'/')]
    if submounts:
        raise ValueError('nested mounts are excluded; refusing this node: '+str(submounts))
    return dict(host=socket.gethostname(), boot=Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
                inode=inode, pid=os.getpid())


def ssh(node, command):
    return ['ssh','-T','-o','BatchMode=yes','-o','ConnectTimeout=5',
            '-o','ServerAliveInterval=5','-o','ServerAliveCountMax=3',node,command]


def probe(node, source):
    code = ('import os,json,socket,pathlib;'
        'p='+repr(source)+';r=os.stat(p,follow_symlinks=False);'
        'print(json.dumps(dict(host=socket.gethostname(),inode=r.st_ino,'
        'boot=pathlib.Path("/proc/sys/kernel/random/boot_id").read_text().strip(),'
        'submounts=[x for x in pathlib.Path("/proc/self/mountinfo").read_text().splitlines() '
        'if x.split()[4].startswith(p+"/")])))')
    result = subprocess.run(ssh(node,'python3 -S -B -'), input=code,
        text=True,capture_output=True,timeout=15)
    if result.returncode:
        raise RuntimeError(node+': '+result.stderr[-400:])
    result = json.loads(result.stdout)
    if result['submounts']:
        raise ValueError('nested mount: '+node)
    result['node'] = node
    return result


class DuProgress:
    """Post-order du --all records, counting child bytes only once; O(depth) state."""
    def __init__(self, names):
        self.roots = set(names)
        self.pending = collections.defaultdict(int)
        self.items = self.bytes = self.total = 0
        self.root_totals = {}

    def record(self, name, size):
        components(name)
        self.items += 1
        self.bytes += size-self.pending.pop(name, 0)
        if name in self.roots:
            self.total += size
            self.root_totals[name] = size
        else:
            self.pending[name.rpartition('/')[0]] += size


def scan_job(source, task, cancel, progress, update, children, child_lock):
    parent = task['parent']
    names = task['names']
    components(parent)
    if not names or len(set(names)) != len(names):
        raise ValueError('invalid sibling operands')
    for name in names:
        if len(components(name)) != 1:
            raise ValueError('operand is not one name')
    fd = directory(source, parent)
    p = None
    try:
        p = subprocess.Popen(['du','--all','--null','--block-size=1','--count-links',
            '--one-file-system','--files0-from=-'],cwd='/proc/self/fd/'+str(fd),pass_fds=(fd,),
            stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,
            start_new_session=True,env=dict(os.environ,LC_ALL='C'))
        with child_lock:
            children.add(p)
        errors = []
        error_bytes = [0]
        def drain_errors():
            for chunk in iter(lambda: p.stderr.read(4096), b''):
                error_bytes[0] += len(chunk)
                if sum(map(len, errors)) < 16000:
                    errors.append(chunk[:4096])
        def feed():
            try:
                p.stdin.write(b''.join(os.fsencode(name)+b'\0' for name in names))
                p.stdin.close()
            except (BrokenPipeError, OSError):
                pass
        reader = threading.Thread(target=drain_errors,daemon=True)
        writer = threading.Thread(target=feed,daemon=True)
        reader.start(); writer.start()
        meter = DuProgress(names)
        rows = []
        buffer = b''
        tick = time.monotonic()
        while True:
            chunk = p.stdout.read1(262144)
            if not chunk:
                break
            buffer += chunk
            records = buffer.split(b'\0')
            buffer = records.pop()
            for raw in records:
                number, path = raw.split(b'\t',1)
                name = os.fsdecode(path)
                size = int(number)
                meter.record(name,size)
                rel = parent+'/'+name if parent else name
                if len(components(rel)) <= 5:
                    try:
                        if stat.S_ISDIR(os.stat(name,dir_fd=fd,follow_symlinks=False).st_mode):
                            rows.append([rel,size])
                    except OSError as exc:
                        errors.append(str(exc).encode()[:1000])
                if meter.items % 512 == 0 and time.monotonic()-tick >= .25:
                    update(task['id'],meter.items,meter.bytes)
                    tick = time.monotonic()
            if cancel.is_set() and p.poll() is None:
                os.killpg(p.pid,signal.SIGTERM)
        rc = p.wait()
        writer.join(timeout=2); reader.join(timeout=2)
        if buffer:
            raise ValueError('incomplete du output record')
        update(task['id'],meter.items,meter.bytes)
        return dict(kind='done',id=task['id'],rc=rc,items=meter.items,
            bytes=meter.total,observed_bytes=meter.bytes,roots=meter.root_totals,
            rows=rows,error_bytes=error_bytes[0],
            errors=b''.join(errors).decode(errors='replace')[:16000],
            all_operands_returned=set(names)==set(meter.root_totals))
    finally:
        os.close(fd)
        if p is not None:
            if p.poll() is None:
                os.killpg(p.pid,signal.SIGTERM)
            with child_lock:
                children.discard(p)
            for pipe in (p.stdin,p.stdout,p.stderr):
                if pipe is not None and not pipe.closed:
                    pipe.close()


def worker(run, node):
    cfg = read_json(run/'settings.json')
    if hashlib.sha256(Path(__file__).read_bytes()).hexdigest() != cfg['script_sha256']:
        raise ValueError('frozen scanner changed')
    actor = identity(cfg['source'])
    expected = cfg['nodes'][node]
    if any(actor[k] != expected[k] for k in ('host','boot','inode')):
        raise ValueError('node or source identity changed')
    lock = threading.Lock()
    output_lock = threading.Lock()
    jobs = {}
    children = set()
    child_lock = threading.Lock()
    cancel = threading.Event()
    last_ping = [time.monotonic()]
    def emit(value):
        with output_lock:
            print(json.dumps(value,ensure_ascii=True,separators=(',',':')),flush=True)
    def update(uid,items,size):
        with lock:
            jobs[uid] = dict(items=items,bytes=size)
    def execute(task):
        try:
            result = scan_job(cfg['source'],task,cancel,jobs,update,children,child_lock)
            emit(result)
        except BaseException as exc:
            emit(dict(kind='fatal',id=task['id'],error=repr(exc)))
            cancel.set()
        finally:
            with lock:
                jobs.pop(task['id'],None)
    def watch():
        while not cancel.wait(5):
            if time.monotonic()-last_ping[0] > 90:
                cancel.set(); break
            with lock:
                current = dict(jobs)
            emit(dict(kind='progress',jobs=current,**actor))
        with child_lock:
            for process in children:
                if process.poll() is None:
                    try:os.killpg(process.pid,signal.SIGTERM)
                    except ProcessLookupError:pass
    threading.Thread(target=watch,daemon=True).start()
    emit(dict(kind='ready',**actor))
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=cfg['per_node'])
    try:
        for line in sys.stdin:
            message = json.loads(line)
            last_ping[0] = time.monotonic()
            if message['kind'] == 'ping':
                continue
            if message['kind'] in ('stop','finish'):
                break
            if message['kind'] != 'job' or cancel.is_set():
                raise ValueError('unexpected worker message')
            update(message['id'],0,0)
            pool.submit(execute,message)
    finally:
        cancel.set()
        pool.shutdown(wait=True,cancel_futures=True)


def controller(run):
    cfg = read_json(run/'settings.json')
    source = cfg['source']
    if identity(source)['inode'] != cfg['root_inode']:
        raise ValueError('source changed')
    work = queue.Queue(maxsize=512)
    events = queue.Queue()
    halted = threading.Event()
    planned = threading.Event()
    journal_lock = threading.Lock()
    planner_stats = dict(tasks=0,scaffolds=0,errors=0,entries=0)
    jobs_file = (run/'jobs.jsonl').open('a',buffering=1)
    results_file = (run/'results.jsonl').open('a',buffering=1)
    scaffolds_file = (run/'scaffolds.jsonl').open('a',buffering=1)
    uid = [0]
    def issue(parent,names):
        with journal_lock:
            uid[0] += 1
            task = dict(kind='job',id=uid[0],parent=parent,names=names)
            jobs_file.write(json.dumps(task,ensure_ascii=True)+'\n')
            planner_stats['tasks'] += 1
        while not halted.is_set():
            try:work.put(task,timeout=.2);return
            except queue.Full:pass
    def scaffold(rel,size,items,error=None):
        value = dict(kind='scaffold',rel=rel,bytes=size,items=items,error=error)
        with journal_lock:
            planner_stats['scaffolds'] += 1
            planner_stats['errors'] += bool(error)
            scaffolds_file.write(json.dumps(value,ensure_ascii=True)+'\n')
        events.put(('planner',value))
    heavy = set(cfg['heavy'])
    special = cfg['special_flat']
    dirs = queue.Queue()
    dirs.put('')
    root_device = os.stat(source).st_dev
    def expand(rel):
        size = 0; items = 1; error = None; names = []
        try:
            fd = directory(source,rel)
            try:
                size = os.fstat(fd).st_blocks*512
                if os.fstat(fd).st_dev != root_device:
                    raise ValueError('nested filesystem')
                with os.scandir(fd) as entries:
                    for entry in entries:
                        if halted.is_set():return
                        planner_stats['entries'] += 1
                        child = rel+'/'+entry.name if rel else entry.name
                        if rel in special:
                            names.append(entry.name)
                            if len(names)==2048:
                                issue(rel,names);names=[]
                            continue
                        info = entry.stat(follow_symlinks=False)
                        if stat.S_ISDIR(info.st_mode):
                            if info.st_dev != root_device:
                                error = 'nested mount: '+child
                                continue
                            split = (len(components(child)) < 3 or child in heavy or
                                     any(x==child or x.startswith(child+'/') for x in special))
                            if split:
                                dirs.put(child)
                            else:
                                names.append(entry.name)
                                if len(names)==64:
                                    issue(rel,names);names=[]
                        else:
                            size += info.st_blocks*512
                            items += 1
                    if names:issue(rel,names)
            finally:
                os.close(fd)
        except BaseException as exc:
            error = repr(exc)
        scaffold(rel,size,items,error)
    def planner_thread():
        while not halted.is_set():
            try:rel=dirs.get(timeout=.2)
            except queue.Empty:continue
            try:expand(rel)
            finally:dirs.task_done()
    def plan():
        for _ in range(8):
            threading.Thread(target=planner_thread,daemon=True).start()
        dirs.join()
        planned.set()
    threading.Thread(target=plan,daemon=True).start()
    clients = {}
    active = {}
    aggregate = collections.defaultdict(int)
    completed = done_items = done_bytes = scaffold_items = scaffold_bytes = 0
    failed = []
    history = collections.deque(maxlen=400)
    started = time.time()
    def read_client(node,p):
        try:
            for line in p.stdout:
                events.put((node,json.loads(line)))
        except BaseException as exc:
            events.put((node,dict(kind='fatal',error=repr(exc))))
        finally:
            events.put((node,dict(kind='eof',rc=p.wait())))
            p.stdout.close()
    def send(client,message):
        client['p'].stdin.write(json.dumps(message,ensure_ascii=True)+'\n')
        client['p'].stdin.flush()
    for node in cfg['nodes']:
        log = (run/'logs'/f'{node}.stderr').open('w')
        command = 'python3 -S -B '+shlex.quote(str(run/'scanner.py'))+' worker --run '+shlex.quote(str(run))+' --node '+shlex.quote(node)
        p = subprocess.Popen(ssh(node,command),stdin=subprocess.PIPE,stdout=subprocess.PIPE,
                             stderr=log,text=True,bufsize=1)
        clients[node]=dict(p=p,ready=False,last=time.monotonic(),log=log)
        reader=threading.Thread(target=read_client,args=(node,p),daemon=True)
        clients[node]['reader']=reader
        reader.start()
    tick = ping = 0
    phase = 'scanning'
    try:
        while True:
            if (run/'STOP').exists():
                phase='stopped';break
            try:
                node,msg=events.get(timeout=.1)
                kind=msg['kind']
                if kind=='scaffold':
                    value=msg['bytes'];rel=msg['rel']
                    for parent in [rel]+list(ancestors(rel)):
                        if len(components(parent))<=5:aggregate[parent]+=value
                    scaffold_items+=msg['items'];scaffold_bytes+=value
                    if msg['error']:failed.append(msg)
                elif kind=='ready':
                    expected=cfg['nodes'][node]
                    if any(msg[k]!=expected[k] for k in ('host','boot','inode')):
                        raise ValueError('worker identity mismatch: '+node)
                    clients[node].update(ready=True,last=time.monotonic(),actor=msg)
                elif kind=='progress':
                    clients[node]['last']=time.monotonic()
                    for key,progress in msg['jobs'].items():
                        key=int(key)
                        if key in active and active[key]['node']==node:
                            active[key]['progress']=progress
                elif kind=='done':
                    task=active.pop(msg['id'])
                    if task['node']!=node:raise ValueError('lease owner mismatch')
                    results_file.write(json.dumps(dict(msg,node=node),ensure_ascii=True)+'\n')
                    completed+=1;done_items+=msg['items'];done_bytes+=msg['bytes']
                    if msg['rc'] or msg['errors'] or not msg['all_operands_returned']:
                        failed.append(dict(node=node,id=msg['id'],rc=msg['rc'],errors=msg['errors']))
                    for rel,value in msg['rows']:aggregate[rel]=value
                    for name,value in msg['roots'].items():
                        rel=task['parent']+'/'+name if task['parent'] else name
                        for parent in ancestors(rel):
                            if len(components(parent))<=5:aggregate[parent]+=value
                elif kind in ('fatal','eof'):
                    raise RuntimeError(node+': '+str(msg))
            except queue.Empty:pass
            now=time.monotonic()
            if now-ping>=5:
                for client in clients.values():send(client,dict(kind='ping'))
                ping=now
            for node,client in clients.items():
                if not client['ready']:continue
                slots=cfg['per_node']-sum(x['node']==node for x in active.values())
                for _ in range(slots):
                    try:task=work.get_nowait()
                    except queue.Empty:break
                    active[task['id']]=dict(task,node=node,progress=dict(items=0,bytes=0))
                    send(client,task)
            if planned.is_set() and work.empty() and not active and events.empty():
                phase='complete' if not failed else 'finished_with_errors'
            if now-tick>=5 or phase!='scanning':
                observed_items=done_items+scaffold_items+sum(x['progress']['items'] for x in active.values())
                observed_bytes=done_bytes+scaffold_bytes+sum(x['progress']['bytes'] for x in active.values())
                history.append((time.time(),observed_items,observed_bytes))
                rates={}
                for seconds in (60,300):
                    older=next((x for x in history if x[0]>=time.time()-seconds),history[0])
                    elapsed=time.time()-older[0]
                    rates[str(seconds)]=dict(seconds=round(elapsed,1),
                        items_per_s=(observed_items-older[1])/elapsed if elapsed>1 else None,
                        allocated_bytes_per_s=(observed_bytes-older[2])/elapsed if elapsed>1 else None)
                status=dict(state=phase,updated_beijing=stamp(),updated=time.time(),
                    source=source,report_depth=5,controller_pid=os.getpid(),started=started,
                    nodes_ready=sum(c['ready'] for c in clients.values()),nodes_total=len(clients),
                    per_node=cfg['per_node'],active=len(active),queued=work.qsize(),
                    producer_done=planned.is_set(),planned_tasks=planner_stats['tasks'],
                    completed_tasks=completed,measured_entries=observed_items,
                    measured_allocated_bytes=observed_bytes,completed_allocated_bytes=aggregate[''],
                    rates=rates,errors=len(failed),planner=dict(planner_stats),
                    workers={n:dict(ready=c['ready'],age=round(now-c['last'],1),
                        actor=c.get('actor'),active=sum(x['node']==n for x in active.values()))
                        for n,c in clients.items()})
                write_json(run/'status.json',status)
                (run/'PROGRESS.md').write_text(format_status(status)+'\n')
                with (run/'samples.jsonl').open('a') as stream:
                    stream.write(json.dumps(status)+'\n')
                tick=now
            if phase!='scanning':break
    except BaseException as exc:
        failed.append(dict(controller_error=repr(exc)))
        phase='failed'
        write_json(run/'first-error.json',dict(at=stamp(),error=repr(exc)))
        current=read_json(run/'status.json') if (run/'status.json').exists() else {}
        current.update(state=phase,updated=time.time(),updated_beijing=stamp(),errors=len(failed))
        write_json(run/'status.json',current)
        (run/'PROGRESS.md').write_text(format_status(current)+'\n')
    finally:
        halted.set()
        for client in clients.values():
            try:send(client,dict(kind='finish' if phase.startswith('complete') else 'stop'))
            except (BrokenPipeError,OSError):pass
            try:client['p'].stdin.close()
            except OSError:pass
        for client in clients.values():
            client['reader'].join(timeout=2)
            client['log'].close()
        write_json(run/'errors.json',failed)
        with (run/'directories-depth5.csv').open('w',newline='',encoding='utf-8',errors='backslashreplace') as stream:
            writer=csv.writer(stream)
            writer.writerow(['depth','path','allocated_bytes_path_sum','allocated_TB','scan_state'])
            for rel,value in sorted(aggregate.items()):
                writer.writerow([len(components(rel)),source+('/'+rel if rel else ''),value,
                                 f'{value/1e12:.6f}',phase])
        lines=['# /share-mixed/projset 容量分布（depth ≤5）','',
               '状态：'+phase+'；更新：'+stamp(),'',
               '口径：GNU du 分配字节，含目录元数据；硬链接按路径累计，不跟随符号链接。',
               '在线遍历，不是原子快照；父子目录不能相加。错误详见 errors.json。','']
        for depth in range(1,6):
            lines+=['## depth='+str(depth),'']
            for rel,value in sorted(((r,v) for r,v in aggregate.items() if len(components(r))==depth),key=lambda x:-x[1])[:25]:
                lines.append('- '+f'{value/1e12:.3f} TB  `'+source+'/'+rel+'`')
            lines.append('')
        (run/'REPORT.md').write_text('\n'.join(lines)+'\n')
        jobs_file.close();results_file.close();scaffolds_file.close()


def format_status(value):
    lines=['projset 全局扫描 | '+value.get('updated_beijing',''),
        '状态：'+value.get('state','unknown')+' | 展示 depth≤5，扫描全部下级',
        f"节点 {value.get('nodes_ready',0)}/{value.get('nodes_total',0)} | 活跃 {value.get('active',0)} | 待领 {value.get('queued',0)} | 错误 {value.get('errors',0)}",
        f"已完成任务 {value.get('completed_tasks',0):,} / 已发现 {value.get('planned_tasks',0):,}（不是全量百分比）",
        f"已计量 {value.get('measured_entries',0):,} 条目 / {value.get('measured_allocated_bytes',0)/1e12:.3f} TB 路径分配量",
        '全量发现：'+('已结束' if value.get('producer_done') else '进行中，范围分母尚未确定')]
    for seconds,label in [('60','近1分钟'),('300','近5分钟')]:
        rate=value.get('rates',{}).get(seconds,{})
        items=rate.get('items_per_s'); size=rate.get('allocated_bytes_per_s')
        lines.append(label+'：'+('采样中' if items is None else
            f'{items:,.0f} 条目/s；{size/1e9:.3f} GB/s 统计量；有效窗口 {rate["seconds"]:.0f}s'))
    lines.append('速度是元数据计量吞吐，不是数据搬运/磁盘读带宽；完成前容量为当前已观察量。')
    return '\n'.join(lines)


def start(nodes,per_node):
    if not 1<=per_node<=16:raise ValueError('per-node must be 1..16')
    if POINTER.exists():
        old=read_json(POINTER)
        try:
            os.kill(old['pid'],0)
        except ProcessLookupError:
            pass
        else:
            raise RuntimeError('existing controller PID still alive; inspect '+old['run'])
    root=identity(SOURCE)
    with concurrent.futures.ThreadPoolExecutor(max_workers=18) as pool:
        results=list(pool.map(lambda n:probe(n,SOURCE),nodes))
    if any(r['inode']!=root['inode'] for r in results):raise ValueError('source inode differs')
    if len({r['boot'] for r in results})!=len(results):raise ValueError('duplicate boot domains')
    heavy=[]
    snapshot=REPO/'docs/analysis/storage-distribution-20261027/exports/directories-depth4-over-1tb-readable.csv'
    if snapshot.exists():
        with snapshot.open(encoding='utf-8-sig',newline='') as stream:
            for row in csv.DictReader(stream):
                if row['path'].startswith(SOURCE+'/'):
                    rel=row['path'][len(SOURCE)+1:]
                    if len(components(rel))<=4:heavy.append(rel)
    special=['eai_pwm/datasets/action_processed_r2/_work/molmobot_stage1_multiview_20261009_v1/shared_visual_cache/frame_cache']
    special=[r for r in special if os.path.isdir(SOURCE+'/'+r)]
    run=REPO/'runs'/('projset-usage-'+datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'-'+uuid.uuid4().hex[:6])
    (run/'logs').mkdir(parents=True)
    raw=Path(__file__).read_bytes()
    (run/'scanner.py').write_bytes(raw)
    cfg=dict(source=SOURCE,root_inode=root['inode'],nodes={r['node']:r for r in results},
             per_node=per_node,heavy=heavy,special_flat=special,
             script_sha256=hashlib.sha256(raw).hexdigest(),created=stamp())
    write_json(run/'settings.json',cfg)
    write_json(run/'status.json',dict(state='starting',updated_beijing=stamp(),updated=time.time(),
        nodes_total=len(nodes),nodes_ready=0,planned_tasks=0,completed_tasks=0))
    log=(run/'controller.log').open('ab')
    process=subprocess.Popen([sys.executable,'-S','-B',str(run/'scanner.py'),'controller','--run',str(run)],
        stdin=subprocess.DEVNULL,stdout=log,stderr=log,start_new_session=True)
    write_json(run/'controller.json',dict(pid=process.pid,created=stamp()))
    write_json(POINTER,dict(run=str(run),pid=process.pid,source=SOURCE))
    print(json.dumps(dict(run=str(run),pid=process.pid,nodes=len(nodes),max_parallel=len(nodes)*per_node)))


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command',choices=['start','controller','worker','status'])
    parser.add_argument('--run',type=Path)
    parser.add_argument('--node')
    parser.add_argument('--nodes',nargs='+',default=['master-0']+[f'worker-{i}' for i in range(17)])
    parser.add_argument('--per-node',type=int,default=8)
    parser.add_argument('--watch',action='store_true')
    args=parser.parse_args()
    if args.command=='start':return start(args.nodes,args.per_node)
    run=(args.run or Path(read_json(POINTER)['run'])).resolve()
    if args.command=='controller':return controller(run)
    if args.command=='worker':return worker(run,args.node)
    while True:
        value=read_json(run/'status.json')
        if args.watch:print('\033[2J\033[H',end='')
        print(format_status(value),flush=True)
        print('实时文档：'+str(run/'PROGRESS.md'))
        print('最终结果：'+str(run/'REPORT.md')+' / directories-depth5.csv')
        if not args.watch or value.get('state') in ('complete','finished_with_errors','failed','stopped'):break
        time.sleep(5)


if __name__=='__main__':main()
