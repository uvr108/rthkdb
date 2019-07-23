import os

rt_host=os.environ['rt_host']
rt_port=os.environ['rt_port']
rt_db=os.environ['rt_db']

import rethinkdb as r
from rethinkdb.errors import RqlRuntimeError
from rthkdb.continuos import Continuos
import sys

def genera(cons, zona, kw):

    for l in cons.consultar(kw):

        fecha_origen = l['DATA']['HYPO']['fecha_origen']
        latitud = float(l['DATA']['HYPO']['latitud'])
        longitud = float(l['DATA']['HYPO']['longitud'])
        magnitud = float(l['DATA']['HYPO']['magnitud'])
        no = l['DATA']['HYPO']['no']
        sfile= l['SFILE']
        yr = l['DATA']['YEAR']
        jl = l['DATA']['JL']
        
        cursor = r.table("continuos").get_all([yr,jl],index='yrjulian').pluck('station','network').distinct().run(conn)

        cont_5 = 0
        cont_10 = 0
        cont_15 = 0
        cont_20 = 0
            
        for doc in cursor:
            
            for st in r.table('estaciones').get_all([doc['network'],doc['station']],index='netcodigo').run(conn):
                diff = abs(float(latitud) -float(st['lat']))
                if diff <= 20:
                    cont_20+=1
                if diff <= 15:
                    cont_15+=1
                if diff <= 10:
                    cont_10+=1
                if diff <= 5:
                    cont_5+=1
                       
        co = {}
        co.update({"sfile":sfile})
        co.update({"yr":yr})
        co.update({"jl":jl})
        co.update({"fecha_origen":fecha_origen})
        co.update({"latitud":latitud})
        co.update({"longitud":longitud})
        co.update({"no":int(no)})
        co.update({"cont_5":cont_5})
        co.update({"cont_10":cont_10})
        co.update({"cont_15":cont_15})
        co.update({"cont_20":cont_20})

        r.table("informes").insert({"zona":zona,"sfile":co['sfile'],"yr":co['yr'],"jl":co['jl'],"fecha_origen":co['fecha_origen'],
        "latitud":co['latitud'],"longitud":co['longitud'],"no":co['no'],"cont_5":co["cont_5"],"cont_10":co["cont_10"],"cont_15":co["cont_15"],"cont_20":co["cont_20"]}).run(conn) 
        

if __name__ == "__main__":


    conn = r.connect(host=rt_host, port=rt_port, db=rt_db)
    cont=Continuos(conn)
    
    jsjl = {
    'ENE':['001','031'],
    'FEB':['032','059'],
    'MAR':['060','090'],
    'ABR':['091','120'],
    'MAY':['121','151'],
    'JUN':['152','181'],
    'JUL':['182','212'],
    'AGO':['213','243'],
    'SEP':['244','273'],
    'OCT':['274','304'],
    'NOV':['305','334'],
    'DIC':['335','365']
    } 

    lat = {
    'Norte':[-23.5,-17.0,'latitud'],
    'N.Chico':[-28.0,-23.5,'latitud'],
    'Zona.C':[-31.0,-28.0,'latitud'],
    'Valpo':[-34.0,-31.0,'latitud'],
    'Sur':[-38.0,-34.0,'latitud'],
    'Ext.Sur':[-49.0,-38.0,'latitud']
    }

    per = sys.argv[1]
    yr = per[0:4]
    jl = per[4:7] 

    
    for k,v in lat.items():
        
        kw={'message': {'table':'eventos','option':'select','between':[v[0],v[1],v[2]], 'where':{'DATA':{'YEAR':yr,'JL':jl}} }}
        genera(cont,k,kw)
        
    conn.close()
    del cont

