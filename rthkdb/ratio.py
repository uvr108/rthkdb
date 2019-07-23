import os

import rethinkdb as r
from rethinkdb.errors import RqlRuntimeError
from rthkdb.continuos import Continuos
import sys

rt_host=os.environ['rt_host']
rt_port=os.environ['rt_port']
rt_db=os.environ['rt_db']


zona = {'Norte':[-23.5,-0.0],'N.Chico':[-28.0,-23.5],'Zona.C':[-31.0,-28.0],'Valpo':[-34.0,-31.0],'Sur':[-38.0,-34.0],'Ext.Sur':[-79.0,-38.0]}

def get_zone(latitud):
   for k,v in zona .items():
       if latitud >= v[0] and latitud < v[1]:
           return k 
    

def genera(cons,kw):

    datajs = {}
    usadas=[0,0,0,0,0,0]
    numero= [0,0,0,0,0,0]
    totales = [0,0,0,0,0,0]

    for l in cons.consultar(kw):

        cont = [0,0,0,0]

        sfile = l.get('SFILE')
        yr = l['DATA']['YEAR']
        jl = l['DATA']['JL']
        latitud = float(l['DATA']['HYPO']['latitud'])
        no = int(l['DATA']['HYPO']['no'])
        mag = l['DATA']['HYPO']['magnitud']
        dep = l['DATA']['HYPO']['prof']
        tipo = l['DATA']['HYPO']['tipo']
        month = (l['DATA']['HYPO']['fecha_origen']).month
        indice = {'indice':[[yr,jl],'yrjulian']}
        pluck = ['network','station']

        z = get_zone(latitud)

        usadas[[*zona.keys()].index(z)]+=no
        numero[[*zona.keys()].index(z)]+=1

        for cur in cons.consultar({'message': {'table':'continuos',  'option':'select','pluck':pluck,  'indice':indice['indice'], 'distinct':True }}):

            indice = {'indice':[[cur['network'],cur['station']],'netcodigo']} 
            pluck = ['codigo','network','lat']

            for st in cons.consultar({'message':{'table':'estaciones','option':'select','indice': indice['indice'], 'distinct': True, 'pluck':pluck }}):
                diff = abs(latitud -float(st['lat']))
                        
                if diff <= 20:
                    cont[3]+=1
                if diff <= 15:
                    cont[2]+=1
                if diff <= 10:
                    cont[1]+=1
                if diff <= 5:
                    cont[0]+=1

        for k in [*zona.keys()]:
            if z==k:
                num_usadas = int(usadas[[*zona.keys()].index(z)]/numero[[*zona.keys()].index(z)]) 
                

                try:
                   r_0 =  float(num_usadas/cont[0])
                except ZeroDivisionError as error:
                   r_0 = 0.0

                try:
                   r_1 =  float(num_usadas/cont[1])
                except ZeroDivisionError as error:
                   r_1 = 0.0

                try:
                   r_2 =  float(num_usadas/cont[2])
                except ZeroDivisionError as error:
                   r_2 = 0.0

                try:
                   r_3 =  float(num_usadas/cont[3])
                except ZeroDivisionError as error:
                   r_3 = 0.0

                ratio = {'5°':float("%.2f" % r_0)}
                ratio.update({'10°':float("%.2f" % r_1)})
                ratio.update({'15°':float("%.2f" % r_2)})
                ratio.update({'20°':float("%.2f" % r_3)})
        
                datajs.update({z : {'eventos':numero[[*zona.keys()].index(z)],'usadas':usadas[[*zona.keys()].index(z)],'ratio':ratio}})
        
    data = {}

    for k,v in datajs.items():

        data.update({k:v})

    return {'yr':yr,'jl':jl,'data':data}


if __name__ == "__main__":

    conn = r.connect(host=rt_host, port=rt_port, db=rt_db) 
    cont=Continuos()
   
    per = sys.argv[1]
    yr = per[0:4]
    jl = per[4:7]

    indice = [[yr,jl],'yrjl']
    
    order = 'SFILE'
        
    print(indice)
 
    pluck = ['SFILE',{'DATA':['JL','YEAR',{'HYPO':['latitud','no','magnitud','prof','tipo','fecha_origen']}]}] 
        
    kw = {'table':'eventos', 'option':'select', 'indice' : indice, 'pluck' : pluck}
     
    r.table('ratio').insert(genera(cont,{'message' : kw})).run(conn)
    
    del(cont)
    conn.close()
