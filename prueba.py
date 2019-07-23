from rthkdb.continuos import Continuos
import rethinkdb as r

import os

rt_host = os.environ['rt_host']
rt_port = os.environ['rt_port']
rt_db = os.environ['rt_db']

if __name__ == "__main__":

    # print('hola mundo')
    cont = Continuos()
    conn = r.connect(host=rt_host, port=rt_port, db=rt_db)

    kw = {}

    tkey = cont.mktkey("2018-03-04T00:00:00", "2018-03-05T23:59:59")

    # flag = ''
    where = {'ano_sfile': 2018, 'mes_sfile': 12}
    # between = {'initial': '2018-12-01T00:00:00+00:00', 'final': '2018-10-01T02:59:59+00:00', 'index': 'fecha_origen'}

    order = ['sfile', 'version']
    # distinct = 0
    pluck1 = ['action', 'fecha_utc', 'latitud', 'longitud', 'dep', 'm1_magnitud', 'm1_tipo']
    pluck2 = ['operator', 'sfile', 'version', 'tipo_estadistica', 'email_origen', 'm20', 'm5']
    pluck3 = ['pup', 'retardo', 'sensible']
    pluck = [*pluck1, *pluck2, *pluck3]

    kw.update({'table': 'triggers', 'command': 'select',  'where': where,
               'order': order, 'pluck': pluck})



    # print(f"{kw}")
    # print(f"{tkey}")

    print("|".join(pluck))


    for c in cont.ejecutar(r, **kw):
        
        print("%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s" % (c['action'], c['fecha_utc'], c['latitud'], c['longitud'], c['dep'], c['m1_magnitud'],
        c['m1_tipo'], c['operator'], c['sfile'], c['version'], c['tipo_estadistica'],
        c['email_origen'], c['m20'], c['m5'], c['pup'], c['retardo'], c['sensible']))
        # print(*c.values())

    conn.close()
    cont.__del__()
