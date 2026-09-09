import json
import time
from pathlib import Path

def save_metrics(path, metrics):
    path=Path(path); path.parent.mkdir(parents=True, exist_ok=True)
    existing=[]
    if path.exists():
        try: existing=json.loads(path.read_text(encoding="utf-8"))
        except Exception: existing=[]
    if not isinstance(existing,list): existing=[]
    existing.append(metrics)
    path.write_text(json.dumps(existing, ensure_ascii=False, indent=2), encoding="utf-8")

def timed(): return time.perf_counter()
