#!/usr/bin/env python3
"""Bounded regular-file copy comparison in an exclusively owned scratch tree."""
import argparse,collections,concurrent.futures,datetime,hashlib,json,os,pathlib,resource,shutil,signal,stat,subprocess,sys,threading,time,uuid

BUFFER=8*1024**2

def utc():return datetime.datetime.now(datetime.timezone.utc).isoformat()
def identity(st):return st.st_ino,st.st_size,st.st_mtime_ns,st.st_ctime_ns

def run(selection,root):
    os.umask(0o077)
    control=pathlib.Path(selection['control']);sys.path.insert(0,str(control))
    from migration_io import read_json,write_json
    import migration_worker as engine
    cfg=read_json(control/'config.json')
    source=pathlib.Path(selection['source'])
    assert str(source)==cfg['source']
    if (control/'STOP').exists():raise RuntimeError('Training STOP; no benchmark')
    health=read_json(control/'training-health.json')
    if health.get('state')=='copy_stopped_to_protect_training':raise RuntimeError('Training protection; no benchmark')
    root=pathlib.Path(root)
    assert root.parent==pathlib.Path('/share-mixed/project') and root.name.startswith('.hcr-copy-benchmark-')
    assert not root.exists() and not root.is_symlink()
    root.mkdir(mode=0o700)
    owner=uuid.uuid4().hex;(root/'OWNER').write_text(owner+'\n')
    results={'version':1,'started':utc(),'node':selection['node'],'source':str(source),'scratch':str(root),'parallelism':64,'rounds':2,'timed_source_sha256':selection.get('timed_source_sha256',False),'zero_extent_probe':'baseline_only','tools':selection.get('tools',{}),'trials':[],
             'conditions':['Production migration continues concurrently; no global cache eviction.','Common source SHA256 baseline warms source cache; no cold-cache claim.','Regular non-sparse independent files only; parent scaffolding precreated untimed.','Python native copy includes target SHA256 and file fsync; cp/rsync native does not.','Comparable total includes fresh source SHA256, target SHA256 and file fsync; source SHA is streamed during Python copy, an extra timed source pass for cp/rsync when timed_source_sha256=true.','Two orders: python/cp/rsync then rsync/cp/python; all targets initially empty.']}
    write_json(root/'results.json',results)
    stopped=threading.Event();children=set();children_lock=threading.Lock()
    def check():
        if stopped.is_set() or (control/'STOP').exists():
            stopped.set();raise RuntimeError('STOP or benchmark cancelled; no further copy')
    def monitor():
        while not stopped.wait(1):
            if (control/'STOP').exists():stopped.set()
        with children_lock:
            for proc in list(children):
                if proc.poll() is None:
                    try:os.killpg(proc.pid,signal.SIGTERM)
                    except ProcessLookupError:pass
    threading.Thread(target=monitor,daemon=True).start()
    engine.configure_rate_limit(20_000_000_000)
    engine.configure_cancel_event(stopped)
    engine.configure_progress_callback(None)
    def hash_source(entry, detect_zeroes=True):
        check();rel=entry['rel'];parts=pathlib.PurePosixPath(rel).parts
        assert rel and not rel.startswith('/') and all(p not in ('..','.') for p in parts)
        p=source/rel;fd=os.open(p,os.O_RDONLY|os.O_NOFOLLOW)
        try:
            before=os.fstat(fd)
            assert stat.S_ISREG(before.st_mode) and before.st_nlink==1
            assert identity(before)==tuple(entry[k] for k in ('ino','size','mtime_ns','ctime_ns'))
            digest=hashlib.sha256();zero=total=0
            while True:
                if stopped.is_set():raise RuntimeError('benchmark stopped')
                block=os.read(fd,BUFFER)
                if not block:break
                digest.update(block);total+=len(block)
                if detect_zeroes and block.count(0)==len(block):zero+=len(block)
            if identity(os.fstat(fd))!=identity(before) or identity(os.stat(p,follow_symlinks=False))!=identity(before):raise RuntimeError('source changed during baseline')
            if detect_zeroes and zero>max(65536,total//100):raise RuntimeError('sample has substantial zero extents')
            out=dict(entry);out['sha256']=digest.hexdigest();out['atime_ns']=os.fstat(fd).st_atime_ns
            return out
        finally:os.close(fd)
    workloads={}
    for name,w in selection['workloads'].items():
        entries=w['entries'];valid=[];excluded=[]
        with concurrent.futures.ThreadPoolExecutor(max_workers=64) as pool:
            futures={pool.submit(hash_source,e):e for e in entries}
            for f in concurrent.futures.as_completed(futures):
                try:valid.append(f.result())
                except Exception as exc:excluded.append({'rel':futures[f]['rel'],'error':str(exc)})
        valid.sort(key=lambda e:e['rel'])
        if not valid:raise RuntimeError('no stable source sample for '+name)
        workloads[name]={'entries':valid,'files':len(valid),'bytes':sum(e['size'] for e in valid),'excluded':excluded}
    write_json(root/'source-baseline.json',workloads)
    results['workloads']={name:{k:v for k,v in w.items() if k!='entries'} for name,w in workloads.items()}
    write_json(root/'results.json',results)
    def verify_target(entry,destination):
        check()
        if selection.get('timed_source_sha256'):
            fresh=hash_source(entry,detect_zeroes=False)
            if fresh['sha256']!=entry['sha256']:raise RuntimeError('fresh source SHA256 changed')
        p=destination/entry['rel'];fd=os.open(p,os.O_RDONLY|os.O_NOFOLLOW)
        try:
            before=os.fstat(fd);assert before.st_size==entry['size'] and stat.S_ISREG(before.st_mode)
            h=hashlib.sha256()
            while True:
                if stopped.is_set():raise RuntimeError('benchmark stopped')
                b=os.read(fd,BUFFER)
                if not b:break
                h.update(b)
            if h.hexdigest()!=entry['sha256']:raise RuntimeError('target SHA256 mismatch: '+entry['rel'])
            if identity(os.fstat(fd))!=identity(before):raise RuntimeError('target changed during verification')
            os.utime(fd,ns=(entry['atime_ns'],entry['mtime_ns']))
            os.fsync(fd)
            st=os.fstat(fd)
            for key in ('size','uid','gid','mtime_ns'):
                if getattr(st,'st_'+key)!=entry[key]:raise RuntimeError('target metadata mismatch '+key)
            if stat.S_IMODE(st.st_mode)!=stat.S_IMODE(entry['mode']):raise RuntimeError('target mode mismatch')
        finally:os.close(fd)
    def stable(entry):return identity(os.stat(source/entry['rel'],follow_symlinks=False))==tuple(entry[k] for k in ('ino','size','mtime_ns','ctime_ns'))
    def usage():
        a=resource.getrusage(resource.RUSAGE_SELF);b=resource.getrusage(resource.RUSAGE_CHILDREN)
        return a.ru_utime+a.ru_stime+b.ru_utime+b.ru_stime
    try:
        for name,w in workloads.items():
            entries=w['entries'];shards=[entries[i::64] for i in range(min(64,len(entries)))]
            lists=root/'lists'/name;lists.mkdir(parents=True)
            for index,group in enumerate(shards):
                (lists/(str(index)+'.nul')).write_bytes(b''.join(os.fsencode(e['rel'])+b'\0' for e in group))
            for round_no,methods in enumerate((('python','cp','rsync'),('rsync','cp','python')),1):
                for method in methods:
                    check()
                    if not all(stable(e) for e in entries):raise RuntimeError('source sample changed; abort comparison')
                    destination=root/('data-'+name+'-'+str(round_no)+'-'+method)
                    destination.mkdir()
                    for parent in {str(pathlib.PurePosixPath(e['rel']).parent) for e in entries}:(destination/parent).mkdir(parents=True,exist_ok=True)
                    ends=[];warnings=[];started=time.perf_counter();cpu_start=usage()
                    def shard_job(pair):
                        check();index,group=pair
                        if method=='python':
                            for entry in group:
                                check();r=engine.copy_entry(str(source),str(destination),entry,owner,verify=True)
                                if r['status']!='copied' or r.get('sha256')!=entry['sha256']:raise RuntimeError('Python copy failed: '+str(r))
                                if r.get('metadata_warnings'):warnings.extend(r['metadata_warnings'])
                                st=os.stat(destination/entry['rel'],follow_symlinks=False)
                                if any(getattr(st,'st_'+k)!=entry[k] for k in ('size','uid','gid','mtime_ns')) or stat.S_IMODE(st.st_mode)!=stat.S_IMODE(entry['mode']):raise RuntimeError('Python destination metadata mismatch')
                            ends.append(time.perf_counter());return
                        if method=='cp':
                            args=[selection.get('executables',{}).get('cp','cp'),'-a','--reflink=never','--sparse=auto','--parents','--']+[e['rel'] for e in group]+[str(destination)]
                        else:
                            args=[selection.get('executables',{}).get('rsync','rsync'),'-aAXS','--whole-file','--from0','--files-from='+str(lists/(str(index)+'.nul')),'--',str(source)+'/',str(destination)+'/']
                        env=os.environ.copy()
                        if selection.get('library_path'):env['LD_LIBRARY_PATH']=selection['library_path']
                        proc=subprocess.Popen(args,cwd=source,env=env,stdout=subprocess.PIPE,stderr=subprocess.PIPE,start_new_session=True)
                        with children_lock:children.add(proc)
                        try:
                            out,err=proc.communicate(timeout=300)
                            if proc.returncode:raise RuntimeError(method+' failed '+str(proc.returncode)+': '+err.decode(errors='replace')[-1500:])
                            ends.append(time.perf_counter())
                        except BaseException:
                            if proc.poll() is None:os.killpg(proc.pid,signal.SIGKILL);proc.wait()
                            raise
                        finally:
                            with children_lock:children.discard(proc)
                        for entry in group:verify_target(entry,destination)
                    errors=[]
                    with concurrent.futures.ThreadPoolExecutor(max_workers=64) as pool:
                        futures=[pool.submit(shard_job,p) for p in enumerate(shards)]
                        for f in concurrent.futures.as_completed(futures):
                            try:f.result()
                            except Exception as exc:errors.append(type(exc).__name__+': '+str(exc))
                    finished=time.perf_counter();check()
                    unchanged=all(stable(e) for e in entries)
                    result={'workload':name,'round':round_no,'method':method,'files':len(entries),'bytes':w['bytes'],'parallelism':min(64,len(shards)),
                            'native_seconds':max(ends)-started if ends else None,'native_includes_verification':method=='python','total_seconds':finished-started,
                            'cpu_seconds':usage()-cpu_start,'verified_GiB_s':w['bytes']/1024**3/(finished-started),'verified_files_s':len(entries)/(finished-started),
                            'source_unchanged':unchanged,'errors':errors,'metadata_warnings':warnings,'valid':unchanged and not errors and not warnings,'finished':utc()}
                    results['trials'].append(result);write_json(root/'results.json',results)
                    print(json.dumps(result),flush=True)
                    # Only this exact fresh target is removed; source and migration target are never cleanup roots.
                    assert (root/'OWNER').read_text().strip()==owner and destination.parent==root and destination.name.startswith('data-') and not destination.is_symlink()
                    shutil.rmtree(destination)
                    if errors or not unchanged:raise RuntimeError('benchmark invalid; results retained')
                    # Brief pause lets storage writeback settle without clearing shared caches.
                    stopped.wait(2)
        results.update(status='complete',finished=utc());write_json(root/'results.json',results)
    except BaseException as exc:
        results.update(status='failed',error=type(exc).__name__+': '+str(exc),finished=utc());write_json(root/'results.json',results);raise
    finally:stopped.set()
    return results

def main():
    p=argparse.ArgumentParser();p.add_argument('--selection',required=True);p.add_argument('--root',required=True);a=p.parse_args()
    selection=json.loads(pathlib.Path(a.selection).read_text());run(selection,a.root)

if __name__=='__main__':main()
