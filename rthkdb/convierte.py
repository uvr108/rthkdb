import rethinkdb as r
from rethinkdb.errors import RqlRuntimeError
from datetime import timedelta, datetime, timezone
import pytz
import os

rt_host = os.environ['rt_host']
rt_port = os.environ['rt_port']
rt_db = "csn"

try:
    conn = r.connect(host=rt_host, port=rt_port, db=rt_db)
except:
    print("I am unable to connect to the rethinkdb database")

print(f'host={rt_host}, port={rt_port}, db={rt_db}')
for t in r.table('triggers').pluck(['sfile','id','fecha_utc']).run(conn):
    origen = str(t['fecha_utc']).replace('/','-').replace(' ','T') + '+00:00'
    r.table('triggers').filter({'id':t['id']}).update({'fecha_origen': r.iso8601(origen)}).run(conn)
    # print(f'{origen} {t["id"]} ')
conn.close()
