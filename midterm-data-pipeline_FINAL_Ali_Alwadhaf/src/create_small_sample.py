import argparse,csv
def main():
 p=argparse.ArgumentParser(); p.add_argument("--input",required=True); p.add_argument("--output",default="data/orders_sample.csv"); p.add_argument("--rows",type=int,default=100000); a=p.parse_args()
 with open(a.input,"r",encoding="utf-8-sig",newline="",errors="replace") as fi,open(a.output,"w",encoding="utf-8",newline="") as fo:
  r=csv.reader(fi); w=csv.writer(fo); header=next(r); w.writerow(header)
  for i,row in enumerate(r):
   if i>=a.rows: break
   w.writerow(row)
 print(f"Created {a.output} with up to {a.rows} data rows")
if __name__=="__main__": main()
