import rethinkdb as r
from rethinkdb.errors import RqlRuntimeError
from datetime import timedelta, datetime, timezone
import pytz
import os

rt_host = '10.54.217.83'
rt_port = os.environ['rt_port']
rt_db = "csn"

conn = r.connect(host=rt_host, port=rt_port, db=rt_db)


for t in r.table('triggers').filter({'ano_sfile':2019}).pluck(['sfile','action','fecha_utc','operador','latitud','longitud','m1_magnitud',\
'm1_tipo','no','m5','m20','email_origen','retardo','pup','operator','latitud','longitud','sensible','tipo_estadistica','version']).\
order_by(r.desc('fecha_utc'),r.desc('version')).run(conn):

    origen = str(t['fecha_utc']).replace('/','-').replace(' ','T') + '+00:00'

    
    if t['sensible']=='yes':
        sensible = True
    else:
        sensible = None

    if t['m5']=='yes':
        m5 = True
    else:
        m5 = None

    if t['m20']=='yes':
        m20 = True
    else:
        m20 = None

    if t['tipo_estadistica'] == 'preliminar':
        up = None
    else:
        if t['pup']=='yes':
            up = 0 
        else:
            up = 'X'    


    """ 18-1330-29L.S201907 """ 

    f = t['sfile']

    da = f[0:2]
    hr = f[3:5]
    mi = f[5:7]
    se = f[8:10]
    yr = f[13:17]
    mo = f[17:21]

    oid = f'{yr}{mo}{da}{hr}{mi}{se}'

    latitud = float(t['latitud'])
    longitud = float(t['longitud'])
    m1_magnitud = float(t['m1_magnitud'])
    retardo = float(t['retardo'])
    version = int(t['version'])

    if t['email_origen'] == None:
        email_origen = None 
    else:
        email_origen = float(t['email_origen'])

    myjs = {'sfile':t['sfile'],'action':t['action'].strip(), 'fecha_origen':r.iso8601(origen), 'operator':t['operator'],\
    'latitud':latitud,'longitud':longitud,'m1_magnitud':m1_magnitud,'m1_tipo':t['m1_tipo'],'no':t['no'],\
    'm5':m5,'m20':m20, 'm5':m5, 'm20':m20, 'email_origen':email_origen,'retardo':retardo, 'sensible':sensible,\
    'tipo_estadistica': t['tipo_estadistica'],'up':up, 'version': version, 'oid':oid}

    r.table('migra').insert(myjs).run(conn) 

conn.close()
