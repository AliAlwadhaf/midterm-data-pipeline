from .batch_loader import load_batch
def run_python(path,db,run_id,batch_size): return load_batch(path,db,run_id,batch_size,str(path))
