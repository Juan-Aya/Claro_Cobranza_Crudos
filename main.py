import os
import sys
# Importar ruta de controllers
sys.path.append(os.path.join("src"))
# Importar librerias 
from Imports import *
from utils import *
from Functions import *
from paths import *

# Obterner la colección de rutas a consultar   
with open(os.path.join(path_to_data,"proceso.json")) as archivo_config:
    configuracion = json.load(archivo_config)
archivos = configuracion["ejecuciones"].keys()
coleccion = configuracion["ejecuciones"]

meses = {"01":"enero","02":"febrero","03":"marzo","04":"abril","05":"mayo","06":"junio","07":"julio","08":"agosto","09":"septiembre","10":"octubre","11":"noviembre","12":"diciembre", }

# Iterar las rutas para poceder hacer el proceso de cada una de las rutas 
for i in range(len(list(archivos))):
    key1=list(archivos)[i].strip("'")
    
    # variables globales
    nombre_archivo = coleccion[key1]["varibles"][0] 
    nombre_tabla   = coleccion[key1]["varibles"][1].strip("'")
    path_0         = coleccion[key1]["varibles"][2].strip("'")
    separador = coleccion[key1]["opcion_path"][1].strip("'")
    #limpieza = coleccion[key1]["opcion_path"][2].strip("'")
    cargue_tabla = coleccion[key1]["opcion_cargue_tabla"] 
    asignacion = coleccion[key1]["asignacion"]
    nombre_sistema_operativo = platform.system()
    #crear lista con la ruta 
    try: 
        path_0 = path_0.split("//")
    except:
        path_0 = path_0.split("\\")
    
    # creacion de la ruta segun el sistema operativo 
    if nombre_sistema_operativo == "Windows":    
        if coleccion[key1]["varibles"][3].strip("'") =="L":
          path_0 = os.path.join(l_windows,*path_0)
        else:
            path_0 = os.path.join(z_windows,*path_0)
    elif nombre_sistema_operativo == "Linux":
        if coleccion[key1]["varibles"][3].strip("'") =="L":
          path_0 = os.path.join(l_sever,*path_0)
        else:
            path_0 = os.path.join(z_server,*path_0)
    # Selección de tipo de ruta a leer 
    if  coleccion[key1]["opcion_path"][0].strip("'") == "1":
        path = os.path.join(path_0, anio, mes, dia)
    elif coleccion[key1]["opcion_path"][0].strip("'") == "2":
        path = os.path.join(path_0, anio, mes) 
    elif coleccion[key1]["opcion_path"][0].strip("'") == "3":
        path = os.path.join(path_0,anio)
    elif coleccion[key1]["opcion_path"][0].strip("'") == "4":
        path = path_0
    elif coleccion[key1]["opcion_path"][0].strip("'") == "5":
        path = os.path.join(path_0,f"{meses[mes]}")  
    # Extracion de dicionarios a leer
    dic_fechas = coleccion[key1]["fechas"]
    dic_formatos = coleccion[key1]["formatos"]
    dic_hojas = coleccion[key1]["sheets"]
    columnas = coleccion[key1]["columnas"]
    manual=1  #1 intenta cargar archivos nuevos y los que han generado error
    if __name__ == '__main__':
        # Ejecucion de función primaria 
        scan_folder(path,nombre_tabla,nombre_archivo,dic_fechas,dic_formatos,dic_hojas,separador, cargue_tabla, asignacion, columnas, manual)   
