import rethinkdb as r
from rethinkdb.errors import RqlRuntimeError
from datetime import timedelta, datetime, timezone
import pytz
import os

rt_host = '10.54.217.83'
rt_port = os.environ['rt_port']
rt_db = "csn"

conn = r.connect(host=rt_host, port=rt_port, db=rt_db)

preliminar=[]

for p in  r.table('migra').pluck('sfile','tipo_estadistica','up').filter({'tipo_estadistica':'preliminar'}).run(conn):
    preliminar.append(p['sfile'])

#print(preliminar)

for d in  r.table('migra').pluck('sfile','up','version').run(conn):
    if d['sfile'] in preliminar:
        if d['up'] == 'X':
            print(r.table('migra').filter({'sfile':d['sfile'],'version':d['version']}).update({'up':d['version']-1}).run(conn)) 
            # print(d['sfile'],d['version'],d['up'],'UNO')
    else: 
        if d['up'] == 'X': 
            print(r.table('migra').filter({'sfile':d['sfile'],'version':d['version']}).update({'up':d['version']}).run(conn)) 
            # print(d['sfile'],d['version'],d['up'],'DOS')

#    up.update({d['sfile']:d['up']})

#sfile = '20-0812-53L.S201901'

#if sfile in preliminar:
#   print('Si',sfile,)
#else:
#   print('No')


conn.close()

