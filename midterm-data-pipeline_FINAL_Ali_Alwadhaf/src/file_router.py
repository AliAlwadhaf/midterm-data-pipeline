from pathlib import Path
def route(path,threshold_mb):
 p=Path(path); mb=p.stat().st_size/(1024*1024); engine="python_batch" if mb<=threshold_mb else "pyspark"; reason=f"{mb:.2f} MB <= {threshold_mb} MB" if engine=="python_batch" else f"{mb:.2f} MB > {threshold_mb} MB"; print(f"File: {p} | Size: {mb:.2f} MB | Engine: {engine} | Reason: {reason}"); return engine,mb
