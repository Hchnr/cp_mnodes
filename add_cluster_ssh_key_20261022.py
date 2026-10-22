"""Add the human-supplied public key to the exact current 12-node cluster.

Only the SSH login account's authorized_keys is changed. Existing bytes and
secure permissions are preserved, with an exclusive backup before appending.
No copy/training process, migration control, source or destination is touched.
"""
import argparse
import base64
import concurrent.futures
import datetime
import hashlib
import json
from pathlib import Path
import struct

from migration_io import read_json, write_json
import migration_launch as transport

JOB = '1e56d871-526b-4036-950b-d11340c193ba'
OWNER = 'ybai'
NODES = ['master-0'] + ['worker-' + str(n) for n in range(11)]
KEY = 'ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQDDMXQOwylZw1LoaboduO2L6lMc7G5Bvag/6PyMtKV3QpTW6ds6oIp1LaK+3HoPxklR+EnJI574P5lwQa0feRAzHrfenp2a/Iv6ASnKlGepgUG77qOXeeN0KkJ4VOAnFGqBNDz0oCUbFHIQp9XScEXixkaTsBq+X9DwBP7l5+JzOAcDnf6qNKxaKhm/5WH2Wpx84kQffacXwNg+PKbZDR49YQXFFBIKYmymbdrIH9R++sV76yI593yqrA/+qgAezwl4zdOyJsnfjpd427X+k8ZhcD9TMdnMa46Ok8PuchB2np220r6+UpDa4H3nYYglPWQgla7gsWNLO3MPO47h07pKdW63qDFcnW/7TY9+B46M2c7vyzp8/0TpDWo7BXilgKIGtHNZzy2p4bq1uEpjjcC2GxK1y36HvF4ueXR0Ccb7MYcLfEdrMNmR5lw1vL3h61bLNUZZyceqSoqRcNUfvSbDpcs+VhfI3kU+SvVz5yecaSXfOgpvB58Q+G0j2ra3ynU= root@job-547aa620-9684-48b4-a2da-a26ef088c3ec-master-0'


def fingerprint():
    payload = base64.b64decode(KEY.split()[1], validate=True)
    fields = []
    while payload:
        if len(payload) < 4:
            raise ValueError('incomplete public key')
        n = struct.unpack('>I', payload[:4])[0]
        if len(payload) < n + 4:
            raise ValueError('incomplete public key field')
        fields.append(payload[4:4+n]); payload = payload[4+n:]
    if len(fields) != 3 or fields[0] != b'ssh-rsa' or int.from_bytes(fields[1], 'big') != 65537:
        raise ValueError('unexpected RSA public key encoding')
    return 'SHA256:' + base64.b64encode(hashlib.sha256(base64.b64decode(KEY.split()[1])).digest()).decode().rstrip('=')


REMOTE = r'''import base64,datetime,fcntl,hashlib,json,os,pwd,shlex,shutil,socket,stat,subprocess
key=KEY; expected_host=HOST; phase=PHASE; prior=PRIOR; stamp=STAMP
assert socket.gethostname()==expected_host, 'wrong cluster/node'
account=pwd.getpwuid(os.geteuid()); assert account.pw_name=='root' and os.geteuid()==0, 'unexpected SSH account'
home=account.pw_dir; assert home=='/root', 'unexpected root home'
def meta(s):return {'dev':s.st_dev,'ino':s.st_ino,'uid':s.st_uid,'gid':s.st_gid,'mode':stat.S_IMODE(s.st_mode),'nlink':s.st_nlink}
def secure(s,directory=False):
 assert (stat.S_ISDIR(s.st_mode) if directory else stat.S_ISREG(s.st_mode)), 'unexpected file type'
 assert s.st_uid==os.geteuid() and not s.st_mode&0o022, 'insecure ownership/permissions'
 if not directory:assert s.st_nlink==1, 'authorized_keys must have one hardlink'
def bound(parent,name,fd):
 a=os.fstat(fd);b=os.stat(name,dir_fd=parent,follow_symlinks=False)
 assert (a.st_dev,a.st_ino)==(b.st_dev,b.st_ino), 'path changed during operation'
def content(fd):
 os.lseek(fd,0,os.SEEK_SET);parts=[];size=0
 while True:
  chunk=os.read(fd,65536)
  if not chunk:return b''.join(parts)
  size+=len(chunk);assert size<=16*1024*1024, 'authorized_keys size limit'
  parts.append(chunk)
def matches(data):
 result=[];typ,blob=key.split()[:2]
 for index,line in enumerate(data.decode('utf-8','surrogateescape').splitlines(),1):
  if not line.strip() or line.lstrip().startswith('#'):continue
  try:tokens=shlex.split(line,comments=False)
  except ValueError:continue
  for pos in (0,1):
   if len(tokens)>pos+1 and tokens[pos]==typ and tokens[pos+1]==blob:
    result.append({'line':index,'options_present':pos==1});break
 return result
flags=os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW|os.O_CLOEXEC
hfd=os.open(home,flags);secure(os.fstat(hfd),True)
result={'host':expected_host,'account':account.pw_name,'home':home,'home_identity':meta(os.fstat(hfd)),
 'authorized_keys_path':home+'/.ssh/authorized_keys','phase':phase,'changed':False}
try:
 try:sfd=os.open('.ssh',flags,dir_fd=hfd)
 except FileNotFoundError:
  if phase!='apply':sfd=None
  else:
   os.mkdir('.ssh',0o700,dir_fd=hfd);sfd=os.open('.ssh',flags,dir_fd=hfd);result['ssh_directory_created']=True
 if sfd is None:result.update(ssh_directory_exists=False,authorized_keys_exists=False,matches=[])
 else:
  try:
   secure(os.fstat(sfd),True);bound(hfd,'.ssh',sfd)
   result.update(ssh_directory_exists=True,ssh_directory_identity=meta(os.fstat(sfd)))
   if phase=='apply':
    assert prior['host']==expected_host and prior['account']=='root' and prior['home']==home
    assert all(prior['home_identity'][k]==result['home_identity'][k] for k in ('dev','ino','uid','gid','mode'))
    if prior['ssh_directory_exists']:
     assert all(prior['ssh_directory_identity'][k]==result['ssh_directory_identity'][k] for k in ('dev','ino','uid','gid','mode'))
   try:fd=os.open('authorized_keys',(os.O_RDWR|os.O_APPEND if phase=='apply' else os.O_RDONLY)|os.O_NOFOLLOW|os.O_CLOEXEC|os.O_NONBLOCK,dir_fd=sfd)
   except FileNotFoundError:
    if phase!='apply':fd=None
    else:
     fd=os.open('authorized_keys',os.O_RDWR|os.O_APPEND|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW|os.O_CLOEXEC,0o600,dir_fd=sfd);result['authorized_keys_created']=True
   if fd is None:result.update(authorized_keys_exists=False,matches=[])
   else:
    try:
     secure(os.fstat(fd));bound(sfd,'authorized_keys',fd)
     if phase=='apply':fcntl.flock(fd,fcntl.LOCK_EX)
     before=content(fd);before_stat=os.fstat(fd);found=matches(before)
     result.update(authorized_keys_exists=True,before_identity=meta(before_stat),before_sha256=hashlib.sha256(before).hexdigest(),before_bytes=len(before),matches_before=found)
     if phase=='apply' and not found:
      backup='authorized_keys.before-add-'+stamp
      backupfd=os.open(backup,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW|os.O_CLOEXEC,0o600,dir_fd=sfd)
      try:
       view=memoryview(before)
       while view:view=view[os.write(backupfd,view):]
       os.fsync(backupfd)
      finally:os.close(backupfd)
      result['backup']=home+'/.ssh/'+backup
      bound(hfd,'.ssh',sfd);bound(sfd,'authorized_keys',fd)
      assert content(fd)==before, 'authorized_keys changed before append'
      addition=(b'' if not before or before.endswith(b'\n') else b'\n')+key.encode()+b'\n'
      written=os.write(fd,addition)
      assert written==len(addition), 'partial append: inspect before retry'
      os.fsync(fd);result['changed']=True
     after=content(fd);bound(sfd,'authorized_keys',fd);bound(hfd,'.ssh',sfd);secure(os.fstat(fd))
     if phase=='apply':assert after.startswith(before) and len(matches(after))>=1, 'readback failed'
     result.update(matches=matches(after),after_identity=meta(os.fstat(fd)),after_sha256=hashlib.sha256(after).hexdigest(),after_bytes=len(after),existing_bytes_preserved=after.startswith(before))
    finally:os.close(fd)
  finally:os.close(sfd)
finally:os.close(hfd)
sshd=shutil.which('sshd') or '/usr/sbin/sshd'
try:
 p=subprocess.run([sshd,'-T'],capture_output=True,text=True,timeout=10)
 result['sshd_effective']={line.split(None,1)[0]:line.split(None,1)[1] for line in p.stdout.splitlines() if line.split(None,1)[0] in ('authorizedkeysfile','pubkeyauthentication','strictmodes')}
 result['sshd_config_check_exit']=p.returncode
except (OSError,subprocess.TimeoutExpired) as exc:result['sshd_config_check_error']=type(exc).__name__
result['observed_at']=datetime.datetime.now(datetime.timezone.utc).isoformat()
print(json.dumps(result))
'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('phase', choices=('preflight', 'apply', 'verify'))
    parser.add_argument('--preflight', type=Path)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    base = Path(__file__).resolve().parent
    active = read_json(base / 'active_migration.json')
    assert active['job'] == JOB and active['ssh_owner'] == OWNER, 'current cluster changed'
    transport.JOB = JOB
    fp = fingerprint()
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
    prior = read_json(args.preflight) if args.preflight else None
    if args.phase == 'apply':
        assert prior and prior['job'] == JOB and prior['fingerprint'] == fp and not prior['errors']
        assert set(prior['nodes']) == set(NODES)
    def one(node):
        code = REMOTE
        for token, value in [('KEY', KEY), ('HOST', 'job-'+JOB+'-'+node), ('PHASE', args.phase),
                             ('PRIOR', prior['nodes'][node] if prior else None), ('STAMP', stamp)]:
            code = code.replace(token, repr(value), 1)
        _, output = transport.ssh(node, code, owner=OWNER)
        return node, json.loads(output)
    result = {'job': JOB, 'owner': OWNER, 'fingerprint': fp, 'phase': args.phase,
              'started_at': datetime.datetime.now(datetime.timezone.utc).isoformat(),
              'nodes': {}, 'errors': [], 'copy_or_training_process_changed': False}
    if args.phase == 'apply':
        # Serial mutations also work if login homes are shared across nodes.
        for node in NODES:
            try:
                _, value = one(node); result['nodes'][node] = value
                write_json(args.output, result)
            except Exception as exc:
                result['errors'].append({'node': node, 'error': str(exc)})
                write_json(args.output, result)
                break
    else:
        with concurrent.futures.ThreadPoolExecutor(max_workers=12) as pool:
            futures = {pool.submit(one, node): node for node in NODES}
            for future in concurrent.futures.as_completed(futures):
                node = futures[future]
                try: _, value = future.result(); result['nodes'][node] = value
                except Exception as exc: result['errors'].append({'node': node, 'error': str(exc)})
    result['finished_at'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    write_json(args.output, result)
    print(json.dumps({'saved': str(args.output), 'phase': args.phase, 'fingerprint': fp,
                      'nodes': len(result['nodes']), 'changed': sum(v['changed'] for v in result['nodes'].values()),
                      'present': sum(bool(v.get('matches')) for v in result['nodes'].values()), 'errors': result['errors']}))
    if result['errors'] or len(result['nodes']) != 12:raise SystemExit(1)


if __name__ == '__main__':
    main()
