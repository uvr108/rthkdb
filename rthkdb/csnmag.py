import rethinkdb as r
from rethinkdb.errors import RqlRuntimeError
from datetime import timedelta, datetime, timezone
import pytz
import os

rt_host = '10.54.217.83'
rt_port = os.environ['rt_port']
rt_db = "csn"

conn = r.connect(host=rt_host, port=rt_port, db=rt_db)





# with open('CSNMAG3.txt','r') as fp:
with open('CSNMAG3.txt','r') as fp:

    for line in fp:
        lin = line[:-1]
        sfile = lin[3:22]
        mag = lin[24:27]
        tip = lin[28:31].strip()
        print(sfile,mag,tip)
        # print(r.table('migra').filter({'sfile':sfile}).max(r.row['up']).update({'m1_magnitud':mag,'m1_tipo':tip}).run(conn)) 
        for d in r.table('migra').pluck('sfile','m1_magnitud','m1_tipo','tipo-estadistica','sensible','up','version').\
        order_by(r.desc('version')).\
        filter((r.row['sfile'].eq(sfile))).limit(1).run(conn): 
            # print('NEW :',d['sfile'],d['version'])
            print(r.table('migra').filter({'sfile': d['sfile'],'version': d['version']}).\
            update({'m1_magnitud':float(mag),'m1_tipo':tip}).run(conn))
        # print(f'|{sfile}|{mag}|{tip}|')
fp.close()

conn.close()
