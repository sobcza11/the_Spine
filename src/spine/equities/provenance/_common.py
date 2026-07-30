import hashlib,json,os,tempfile
from datetime import datetime, timezone
from pathlib import Path
FORBIDDEN_TIME_KEYS={"file_mtime","file_ctime","git_timestamp","execution_time","generated_at"}
def parse_utc(value):
 if not isinstance(value,str) or not value:
  raise ValueError("EQUITIES_PROVENANCE_TIMESTAMP_INVALID")
 try:
  parsed=datetime.fromisoformat(value.replace("Z","+00:00"))
 except ValueError as exc:
  raise ValueError("EQUITIES_PROVENANCE_TIMESTAMP_INVALID") from exc
 if parsed.tzinfo is None or parsed.utcoffset()!=timezone.utc.utcoffset(parsed):
  raise ValueError("EQUITIES_PROVENANCE_TIMESTAMP_NOT_UTC")
 return parsed
def digest(value, excluded=()):
 d={k:v for k,v in value.items() if k not in excluded}
 return hashlib.sha256(json.dumps(d,sort_keys=True,separators=(",",":"),allow_nan=False).encode()).hexdigest()
def file_hash(path): return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def atomic(path,value):
 p=Path(path);p.parent.mkdir(parents=True,exist_ok=True);fd,tmp=tempfile.mkstemp(dir=p.parent,prefix=f".{p.name}.",suffix=".tmp")
 try:
  with os.fdopen(fd,"w",encoding="utf-8",newline="\n") as f: json.dump(value,f,indent=2,sort_keys=True,allow_nan=False);f.write("\n")
  os.replace(tmp,p)
 finally:
  if os.path.exists(tmp):os.unlink(tmp)
def load(path): return json.loads(Path(path).read_text(encoding="utf-8"))
def reject_forbidden(value):
 found=FORBIDDEN_TIME_KEYS & set(value)
 if found: raise ValueError(f"EQUITIES_PROVENANCE_FORBIDDEN_TIMESTAMP:{sorted(found)[0]}")
def utc_order(a,b):
 x,y=parse_utc(a),parse_utc(b)
 if x>y: raise ValueError("EQUITIES_PROVENANCE_TIMESTAMP_INVERSION")
