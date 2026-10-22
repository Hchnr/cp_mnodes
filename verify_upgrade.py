import sys,pathlib,tempfile,os,json,uuid,threading,http.server,urllib.parse,time,sqlite3
sys.path.insert(0,str(pathlib.Path(__file__).parent))
from migration_io import read_json,write_json
import migration_run as run
from migration_broker import Broker
from migration_inventory import prepare_inventory,run_node,aggregate_inventory
from migration_resume_index import build_resume_index
from migration_progress import snapshot
root=pathlib.Path(tempfile.mkdtemp(prefix='upgrade-check-',dir=pathlib.Path(__file__).parent))
src=root/'src';dst=root/'dst';c=root/'control';c.mkdir();(c/'nodes').mkdir();dst.mkdir()
for rel in ('home/hcr/.hidden','repos/cosmos-framework-pwm/code','datasets/data'):
 p=src/rel;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(os.urandom(65536))
p=src/'home/hcr/sparse';f=open(p,'wb');f.seek(8*1024**2);f.write(b'last');f.close()
os.link(p,src/'home/hcr/alias');os.symlink('.hidden',src/'home/hcr/link')
server=http.server.ThreadingHTTPServer(('127.0.0.1',0),http.server.BaseHTTPRequestHandler)
cfg={'run_id':'verify-'+uuid.uuid4().hex,'source':str(src),'destination':str(dst),'threads':4,'broker_url':'http://127.0.0.1:'+str(server.server_port),'broker_token':'test','scan_entries_per_second':100000,'poll_interval_seconds':0.05,'io_bytes_per_second':128*1024**2}
write_json(c/'config.json',cfg);write_json(c/'state.json',{'status':'starting','stage':None})
broker=Broker(c)
class Handler(http.server.BaseHTTPRequestHandler):
 def do_GET(self):
  q=urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
  self.respond(broker.claim(q['stage'][0],q['owner'][0]))
 def do_POST(self):self.respond(broker.done(json.loads(self.rfile.read(int(self.headers['Content-Length'])))))
 def respond(self,v):
  b=json.dumps(v).encode();self.send_response(200);self.send_header('Content-Length',str(len(b)));self.end_headers();self.wfile.write(b)
 def log_message(self,*a):pass
server.RequestHandlerClass=Handler
threading.Thread(target=server.serve_forever,daemon=True).start();run.validate_mounts=lambda c:None
w=threading.Thread(target=run.worker,args=(c,4));w.start();run.Coordinator(c).run();w.join(10)
assert not w.is_alive();s=read_json(c/'state.json');assert s['status']=='complete',s
for rel in ('.hidden','sparse','alias'):assert (src/'home/hcr'/rel).read_bytes()==(dst/'home/hcr'/rel).read_bytes()
assert os.stat(dst/'home/hcr/sparse').st_ino==os.stat(dst/'home/hcr/alias').st_ino
assert os.readlink(dst/'home/hcr/link')=='.hidden'
plan=prepare_inventory(src,c,nodes=2,rate=100)
for i in range(2):run_node(c,i,rate=100)
inv=aggregate_inventory(c);assert inv['complete'] and inv['exact'];write_json(c/'inventory.json',inv)
tmpdb=root/'resume.sqlite';summary=build_resume_index(c,tmpdb)
assert summary['verified_files']>=4,summary
z=snapshot(c);assert z['overall']['inventory_complete'];assert not z['warnings'],z['warnings']
server.shutdown();print(json.dumps({'fixture':str(root),'status':s['status'],'inventory':inv['totals'],'resume':summary,'source_untouched':True}))
