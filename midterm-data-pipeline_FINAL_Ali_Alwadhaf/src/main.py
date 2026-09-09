import argparse,sys,uuid,time
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from config.settings import *
from src.file_router import route
from src.mongo_setup import get_db,setup_collections
from src.batch_loader import load_batch
from src.metrics import save_metrics

def main():
 p=argparse.ArgumentParser(description="Hybrid ELT pipeline for mixed-quality orders")
 p.add_argument("--input",required=True); p.add_argument("--force-engine",choices=["python_batch","pyspark"]); p.add_argument("--mongo-uri",default=MONGO_URI); p.add_argument("--database",default=MONGO_DATABASE); p.add_argument("--batch-size",type=int,default=BATCH_SIZE); p.add_argument("--threshold-mb",type=int,default=SMALL_FILE_THRESHOLD_MB); p.add_argument("--master",default=SPARK_MASTER); a=p.parse_args()
 run_id=str(uuid.uuid4()); started=time.perf_counter(); engine,size=route(a.input,a.threshold_mb); engine=a.force_engine or engine
 db=get_db(a.mongo_uri,a.database); setup_collections(db)
 try:
  if engine=="python_batch": metrics=load_batch(a.input,db,run_id,a.batch_size,a.input)
  else:
   from src.spark_loader import run_spark
   metrics=run_spark(a.input,a.mongo_uri,a.database,run_id,a.master,SPARK_CONNECTOR_PACKAGE)
  metrics.update({"run_id":run_id,"file_name":Path(a.input).name,"file_size_mb":round(size,3),"engine_used":engine,"batch_size":a.batch_size if engine=="python_batch" else None,"threshold_mb":a.threshold_mb,"started_at":time.strftime("%Y-%m-%dT%H:%M:%SZ",time.gmtime()),"total_elapsed_seconds":time.perf_counter()-started})
  save_metrics(REPORT_PATH,metrics); print("METRICS:",metrics)
 finally: db.client.close()
if __name__=="__main__": main()
