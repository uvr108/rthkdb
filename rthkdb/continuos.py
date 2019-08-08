import rethinkdb as r
from rethinkdb.errors import RqlRuntimeError
from datetime import timedelta, datetime, timezone
import pytz
import os
import datetime
from urllib.request import urlopen

rt_host = os.environ['rt_host']
rt_port = os.environ['rt_port']
rt_db = os.environ['rt_db'] 


class Continuos:

    def __init__(self, conn=None):

        if conn == None:
            print(f'connect to : host={rt_host} port={rt_port} db={rt_db}')
            self.conn = r.connect(host=rt_host, port=rt_port, db=rt_db)
        else:
            self.conn = conn

        self.output = {}
        print('Continuos Creado ....')

    def create_index(self, **kw):

        campos = []
        for c in kw['campos']:
            campos.append(r.row[c])

        r.table(kw['table']).index_create(
            kw['indice'], campos).run(self.conn)

        r.table(kw['table']).index_wait(kw['indice']).run(self.conn)

    def mktkey(self, di, df):

        return [datetime.strptime(di, "%Y-%m-%dT%H:%M:%S"), datetime.strptime(df, "%Y-%m-%dT%H:%M:%S")]

    def get_julian(self, period):
        tt=[]
        fmt = '%Y-%m-%d'
        for p in period:

            tt.append([p[:4],"%0.3d" % datetime.datetime.strptime(p, fmt).timetuple().tm_yday])


        return tt


    def consultar(self, kw):

        msg = kw['message']

        if msg.get('table'):
            table = msg['table']
        else:
           return 1

        if msg['option'] == 'select':

            todas = r.table(table)

            if msg.get('indice'):
                todas = todas.get_all(msg['indice'][0], index=msg['indice'][1])

            if msg.get('get_julian'):
                period = msg['get_julian'][0]
                pe = self.get_julian([period[0],period[1]])
                todas = todas.between(pe[0],pe[1],index='yrjl')

            if msg.get('between'):
                todas = todas.between(msg['between'][0],msg['between'][1],index=msg['between'][2])

            if msg.get('betweenISO'):
                todas = todas.between(r.iso8601(msg['betweenISO'][0]), r.iso8601(msg['betweenISO'][1]), index=msg['betweenISO'][2])

            if msg.get('where'):
                todas = todas.filter(msg['where'])

            if msg.get('or'):

                def func(i,maxi):
                    if i==maxi:
                        pass
                    else: 
                        return rkey[i].or_(func(i+1,maxi))
 
                key = [ k for k in msg['or'].keys()][0]
                value = [ v for v in msg['or'].values()][0]

                rkey=[]

                for rng in range(value.__len__()):
                    rkey.append(r.row[key].eq(msg['or'][key][rng]))

                # todas = todas.filter(rkey[0].or_(rkey[1].or_(rkey[2].or_(rkey[3]))))
                todas = todas.filter(func(0,value.__len__()))
               

            if msg.get('order'):
                order=[]
                for k in msg['order']:
                    order.append(k)
                #    order.append(getattr(r,v)(k))
                todas = todas.order_by(*order)

            if msg.get('limit'):
                todas = todas.limit(msg['limit'])

            if msg.get('pluck'):
                todas = todas.pluck(msg['pluck'])

            if msg.get('distinct'):
                todas = todas.distinct()

            for item in todas.run(self.conn):
               yield item

    def ejecutar(self,kw):

        msg = kw['message']

        if msg.get('table'):
            table = msg['table']
        else:
           return 1

        dictio = msg['dictio'] 
        where = {'sfile': dictio['sfile']}
        where2 = {'sfile': dictio['sfile'], 'tipo_estadistica':'final'}

        def contar(tab,prim,seg,ind,wh):
            return r.table(tab).get_all([prim,seg],index=ind).filter(wh).count().run(self.conn)

        def alerta_final(sfile,tipo):

            html = urlopen("http://127.0.0.1:8000/%s/%s" % (sfile,tipo))
            # with open("output.txt",'w') as output:
            #    output.write(html.read().decode("utf-8")) 
            # output.close()

        if msg['option'] == 'insert':


            primero = dictio['ano_sfile']
            segundo = dictio['mes_sfile']
            indice = 'anomes'

            dictio['version'] = contar(table,primero,segundo,indice,where)
            if dictio['tipo_estadistica'] == 'final': 
                dictio['up'] = contar(table,primero,segundo,indice,where2)
                     
            r.table(table).insert(dictio).run(self.conn)

            # +++++++++ MENSAJES GESTION ++++++++++++
            if dictio['up'] == None:
                if dictio['m1_magnitud'] >= 3.7:
                    if dictio['retardo'] > 5.0: 
                        alerta_final(dictio['sfile'],dictio['tipo_estadistica'])

            if dictio['up'] == 0:
                if dictio['m1_magnitud'] >= 3.7:
                    if dictio['retardo'] > 20.0: 
                        alerta_final(dictio['sfile'],dictio['tipo_estadistica']) 
            # ++++++++++++++++++++++++++++++++++++++++

                      
 
        elif msg['option'] == 'update':
           
            """
            msg : {'table': 'analisis', 'option': 'update', 'dictio': {'sfile': '11-1607-03L.S201906', 'yr': 2019, 'mo': 6, 'tipo': 'preliminar', 'email_origen': 10.9, 'sensible': None}}
            """
            primero = dictio['yr']
            segundo = dictio['mo']
            indice = 'anomes'

            setea = {'email_origen': dictio['email_origen']}

            if dictio['tipo_estadistica'] == 'preliminar':
                where = {'sfile': dictio['sfile'],'tipo_estadistica':'preliminar'}
            else:
                version = contar(table,primero,segundo,indice,where2) 
                where = {'sfile': dictio['sfile'],'tipo_estadistica':'final', 'version': version}

            r.table(table).get_all([primero,segundo],index=indice).filter(where).update(setea).run(self.conn)
            
           
        elif msg['option'] == 'sensible': 

            primero = dictio['yr']
            segundo = dictio['mo']
            indice = 'anomes'

            setea = {'sensible': True}

            cons = r.table(table).get_all([primero,segundo],index=indice)
            cons.filter({'sfile':dictio['sfile']}).update(setea).run(self.conn)
            
            # ++++++++ MENSAJES GESTION ++++++++++++

            retardo1 = cons.filter({'sfile':dictio['sfile'],'up':None}).pluck('retardo').run(self.conn)
            retardo2 = cons.filter({'sfile':dictio['sfile'],'up':0}).pluck('retardo').run(self.conn)

            r1 = [ ret['retardo'] for ret in retardo1 ][0]
            r2 = [ ret['retardo'] for ret in retardo2 ][0]
            
            if r1 > 5.0 or r2 > 20.0: 
                alerta_final(dictio['sfile'],'sensible')

            # ++++++++++++++++++++++++++++++++++++++

if __name__ == "__main__":
    pass
