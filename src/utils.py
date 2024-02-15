# Importar librerias necesarias
import os
import sys
import pandas as pd
from Functions import *
from Connections import *
from Connections import MySqlConnections as ms
from etl_asignacion import *
sys.path.append(os.path.join(".."))
from Imports import *

#---- Docuento basado en funciones para trabajar archivos excel y csv y comprimidos ----# 
#-- Función inicial scan_folder
#-- Función segudaria Read_files_path
#-- Función terciaria check_and_add
#-- Funciónes de tratamiento y cargue la informacion a las tablas: toSqlTxt,toSqlExcel
#-- Funciónes de tratamiento de datos en columnas: convertir_fecha,insertar_raya_al_piso 
script_dir = os.path.dirname(os.path.abspath(__file__))
log_file_path = os.path.join(script_dir, 'log.yml')
log_file_path1 = os.path.join(script_dir, '..', 'LoadedFiles')

with open(log_file_path) as f: 
    cnf = yaml.safe_load(f)
    logging.config.dictConfig(cnf)

# Variables globales para el proceso
inicio  = time.time()
anio    = datetime.datetime.now().strftime("%Y")
mes     = datetime.datetime.now().strftime("%m")
dia     = datetime.datetime.now().strftime("%d")
hora    = datetime.datetime.now().strftime("%H:%M")
fecha   = datetime.datetime.now().strftime("%Y-%m-%d")
ayer    = datetime.datetime.now() - datetime.timedelta(days=1)
ayer    = ayer.strftime("%d") 
# Función segundaria del archivo
def Read_files_path(path_,nombre_tabla,nombre_archivo, manual):
    try: 
        cdn_connection,engine,bbdd_or = mysql_connection()
        with engine.connect() as conn:
            if manual==0:
                consulta = text(f"""SELECT CONCAT(FILE_DATE,' - ', FILE_NAME) FROM {bbdd_or}.tb_loaded_files where  TABLA='{nombre_tabla}';""")
            else: 
                consulta = text(f"""SELECT CONCAT(FILE_DATE,' - ', FILE_NAME) FROM {bbdd_or}.tb_loaded_files where ESTADO != 'ERROR' AND TABLA= '{nombre_tabla}';""")
            resultado=conn.execute(consulta) 
            resultado2 =  resultado.fetchall()
        lista_simple = [elemento for sublista in resultado2 for elemento in sublista]
        archivos_coincidentes =[]
        archivos_total = os.listdir(path_) # Listar los archivos del direcctorio 
        for a in nombre_archivo: # Iterar loa archivos del directrio 
            archivos = [valor for valor in archivos_total if f"{a}" in valor] # Selecionar los archivos deseados
            archivos_coincidentes.extend(archivos) # agregar el archivo a la lista 
    except Exception as e:
        logging.getLogger("user").exception(e)
        raise 
    archivos_con_fecha = [f"{datetime.datetime.fromtimestamp(os.path.getmtime(os.path.join(path_, archivo))).strftime('%Y-%m-%d %H:%M:%S')} - {archivo}" for archivo in archivos_coincidentes]
    different_files = set(archivos_con_fecha).difference(lista_simple) # Comparación de los archivos ya cargados y los archivos para cargar así cargar las diferencias  
    if len(different_files) == 0:
        logging.getLogger("user").debug("No hay archivos para cargar")
    logging.getLogger("user").info(different_files)
    return different_files # Archivos a cargar

def insertar_raya_al_piso(cadena): #Funcion que inserta raya al piso cuando el encabezado es CamelCase
        nueva_cadena=""
        try:
            if cadena.isupper() or cadena.islower() or ("_" in cadena):
                nueva_cadena=cadena
            else:
                i=0
                for letra in cadena:
                    if letra.isupper() and i!=0:
                        nueva_cadena =nueva_cadena + "_" + letra
                    else:
                        nueva_cadena = nueva_cadena + letra
                    i+=1
            return nueva_cadena
        except:
            return cadena

def convertir_fecha(fecha):
    formatos = ["%Y-%m-%d %I:%M:%S", "%d/%m/%Y %I:%M:%S", "%Y-%m-%d", "%Y/%m/%d", "%d/%m/%Y", "%m-%d-%y", "%d/%m/%Y:%H:%M:%S", "%d-%m-%Y %H:%M:%S", "%d/%m/%Y 0:00:00", "%d/%m/%Y 00:00:00", "%d/%m/%Y %H:%M:%S","%Y-%m-%d %H:%M:%S","%d/%m/%Y %I:%M:%S %p"]
    for formato in formatos:
        try:
            fecha = pd.to_datetime(fecha, format=formato)
            return str(fecha)
        except Exception as e:
            pass
        # Si no coincide con ninguno de los formatos, intenta manejar el caso especial de fechas cortas
        try:
            if len(str(fecha)) <= 5:
                x = datetime(1900, 1, 1)
                fecha = x + pd.to_timedelta(int(fecha) - 2, unit='D')
                return str(fecha)
        except ValueError:
                pass
                # Si no puede convertir la fecha, devuelve un mensaje indicando el problema
                return "No se pudo convertir la fecha"
            
def toSqlTxt(path,nombre_tabla,file_, dic_fechas, dic_formatos, separador, columnas_tabla):
    try:
        if len(columnas_tabla)==0 and nombre_tabla != 'bdd_asignacion_claro_cobranza':
            df = dd.read_csv(path+"/"+file_[22:],sep = separador,dtype=str, index_col=False)
        elif nombre_tabla == 'bdd_asignacion_claro_cobranza':
            df = pd.read_csv(path+"/"+file_[22:],sep = separador,dtype=str, index_col=False, on_bad_lines='skip',encoding='utf-8', skip_blank_lines=False ,engine='python' , quoting=3,chunksize=15000_000)#
        else:
            df = dd.read_csv(path+"/"+file_[22:],sep = separador,dtype=str, index_col=False, names=columnas_tabla, encoding='latin-1')
        toSqlDf(df, path, file_, dic_fechas, dic_formatos, nombre_tabla)
    except Exception as e:
        print(e)
        raise

def toSqlExcel(path,nombre_tabla,file_, dic_fechas,dic_formatos,dic_hojas,separador, cargue_tabla, asignacion,columnas_tabla):
    try:
        excel_file = pd.ExcelFile(path+"/"+file_[22:])
        hojas = excel_file.sheet_names
        logging.getLogger("user").info(f"Hojas del Excel : {hojas}")
        hojas_documento = dic_hojas
        for sheet in hojas_documento:
            if sheet == "None":
                sheet=0
            try:
                if len(columnas_tabla)==0:
                    df = pd.read_excel(path+"/"+file_[22:], sheet_name = sheet, dtype=str)
                else:
                    df = pd.read_excel(path+"/"+file_[22:],sheet_name = sheet, dtype=str, names=columnas_tabla)
                if cargue_tabla == 1 and len(hojas_documento)>1:
                    df['HOJA_DATA'] = sheet
                    nombre_tabla1 = f"{nombre_tabla}"
                elif cargue_tabla == 0 and len(hojas_documento)>1:
                        nombre_tabla1= f"{nombre_tabla}_{sheet.lower()}"
                else:
                    nombre_tabla1 = f"{nombre_tabla}"
                toSqlDf(df, path, file_, dic_fechas, dic_formatos, nombre_tabla1)
            except Exception as e:
                raise
    except Exception as e:
        raise

def toSqlZip(path,nombre_tabla,file, dic_fechas,dic_formatos,dic_hojas,separador, nombre_archivo, cargue_tabla, asignacion, logs,columnas_tabla):
    try:
        archivo_zip = path+"/"+file[22:]
        logging.getLogger("user").info(f"El archivo a cargar es {archivo_zip}")
        with zipfile.ZipFile(archivo_zip, 'r') as zip_ref:
            zip_ref.extractall(r"/disk/PROCESOS/BASESGOPASS/src/ZIP/")
        file_to_load = Read_files_path(os.path.join("ZIP"),nombre_tabla,nombre_archivo)
        logging.getLogger("user").info("lista de archivos: ",file_to_load)
        for file_1 in file_to_load:
            nombre, extension = os.path.splitext(file_1)
            if extension in [".txt", ".csv"]:
                toSqlTxt(os.path.join(os.getcwd(),"ZIP"),nombre_tabla,file_1, dic_fechas,dic_formatos,separador,columnas_tabla)
            elif extension in [".xlsx"]:
                toSqlExcel(os.path.join("ZIP"),nombre_tabla,file_1, dic_fechas,dic_formatos,dic_hojas,separador, cargue_tabla, asignacion)
    except:
        raise
    finally:
        for file in os.listdir(os.path.join("ZIP")):
            os.remove(os.path.join("ZIP", file))         

def toSqlDf(df, path, file_, dic_fechas, dic_formatos, nombre_tabla):
    try:
        if type(df)==pd.core.frame.DataFrame:
            df=dd.from_pandas(df, npartitions=10)
        fecha= datetime.datetime.fromtimestamp(os.path.getmtime(os.path.join(path, file_[22:])))
        
        tabla_reemplazo = str.maketrans({"á":"a","é":"e","í":"i","ó":"o","ú":"u","ñ":"n","Á":"A","É":"E","Í":"I","Ó":"O","Ú":"U","Ñ":"N"})
        df.columns = df.columns.str.strip()
        df.columns = df.columns.str.replace(" ", "_", regex=True)
        df.columns = df.columns.str.translate(tabla_reemplazo)
        df.columns = df.columns.str.replace("[^0-9a-zA-Z_]", "", regex=True)
        df.columns = [insertar_raya_al_piso(nombre_columna) for nombre_columna in df.columns]
        df.columns = df.columns.str.upper()
        columnas_validar=df.columns.tolist()
        for indice, a in enumerate(columnas_validar):
            if columnas_validar.count(a)>1:
                indices = [i for i, elemento in enumerate(columnas_validar) if elemento == a]
                i=1
                for j in indices:
                    columnas_validar[j]=f"{columnas_validar[j]}_{i}"
                    i=i+1
        df.columns = columnas_validar
        cdn_connection,engine,bbdd_or = mysql_connection()
        with engine.connect() as connection:
            for fecha_mod in dic_fechas:
                if fecha_mod in df.columns:
                    df[fecha_mod]=df[fecha_mod].apply(convertir_fecha) # ,meta=(f"{fecha_mod}", "str")
            df['FILE_DATE'] = fecha                                     
            df['FILE_NAME'] = file_[22:]
            df['FILE_YEAR'] = df['FILE_DATE'].dt.year
            df['FILE_MONTH']  = df['FILE_DATE'].dt.month
            df=df.fillna("None")
            for formato in dic_formatos:
                if formato in df.columns:
                    df[formato] = df[formato].str.replace(",", ".", regex=True)
                    df[formato] = df[formato].str.replace("[^0-9-.]", "", regex=True)
            tabla = Table(f"tb_{nombre_tabla}", MetaData(), autoload_with = engine)
            nombre_columnas_nuevas = [c.name for c in tabla.c]
            diffCols = df.columns.difference(nombre_columnas_nuevas)
            listCols = list(diffCols)
            if len(listCols)!=0:
                for i in range(len(listCols)):
                    logging.getLogger("user").info(f"Columna {i} agregada.")
                    connection.execute(text(f"ALTER TABLE `{bbdd_or}`.`{tabla.name}` ADD COLUMN `" + listCols[i] + "` VARCHAR(128)"))
            tabla = Table(f"tb_{nombre_tabla}", MetaData(), autoload_with = engine)
            path= os.path.join(os.getcwd(),'temp','temp_data.csv')
            csv_file = str(path)
            # Suponiendo que df es tu DataFrame de Dask
            # Escribir el DataFrame de Dask en un archivo CSV con encabezados
            df = pd.concat([df] + [chunk for chunk in df], ignore_index=True)
            df.to_csv(csv_file, sep='德', header=True, index=False, encoding='utf-8',chunksize=1500_000)
            csv_file = str(csv_file).replace('\\\\','\\')

            with engine.connect() as conn:
                columnas= df.columns
                consulta = r"""
                LOAD DATA LOCAL INFILE '{}'
                INTO TABLE tb_{}
                FIELDS TERMINATED BY '德'
                ENCLOSED BY '\"'
                LINES TERMINATED BY '\n'
                IGNORE 1 LINES
                """.format(csv_file,nombre_tabla)
                consulta += f"""({', '.join(columnas)})
                                ON DUPLICATE KEY UPDATE 
                                {', '.join([f"{col} = VALUES({col})" for col in columnas])};"""
                print(consulta)
                conn.execute(consulta)
            # Eliminar el archivo CSV temporal
            if os.path.exists(csv_file):
                os.remove(csv_file)
            valores_filas = df.values.compute().tolist()
            list_of_tuples = [tuple(None if value == "None"  else value for value in row) for row in valores_filas]
            chunks = [list_of_tuples[i:i+10000] for i in range(0, len(list_of_tuples), 10000)]
            argumentos = [(chunk, df.columns, nombre_tabla, engine) for chunk in chunks]
            len(chunks)
            with ThreadPoolExecutor(max_workers=10) as executor:
                executor.map(to_sql_replace, argumentos)
            logging.getLogger("user").info('Insercion Tabla destino DW')
    except Exception as e:
        print(e)
        raise
        

def to_sql_replace(argumentos):
    try:
        chunks, columnas_validar, nombre_tabla, engine=argumentos
        len(chunks)
        tablaDestino = Table(f"tb_{nombre_tabla}", MetaData(), autoload_with = engine)
        data = [dict(zip(columnas_validar, row)) for row in chunks] #Diccionario de columna y valor a agregar
        stmt = insert(tablaDestino).values(data) #Sentencia de insert sqlalchemy
        columns = [column.name  for column in tablaDestino.columns if column.name != "DATA_COZDANIE"] #columnas de tabla para aplicar on duplicate (sin tener en cuenta la de insercion)
        update_values = {col: text(f"VALUES({col})") for col in columns}# Construye dinámicamente la cláusula ON DUPLICATE KEY UPDATE
        with engine.connect() as conn:
            do_update_stmt = conn.execute(stmt.on_duplicate_key_update(**update_values))#Se agrega on duplicate al insert
            logging.getLogger("user").info(f"Registros insertados : {do_update_stmt.rowcount}")
    except Exception as error:   
        raise  

def check_and_add(path,nombre_tabla,file, dic_fechas,dic_formatos,dic_hojas,separador, cargue_tabla, asignacion, nombre_archivo, columnas_tabla):
    logging.getLogger("user").info(f" archivo a cargar{file}")
    ini = time.time() # timepo de inicio del cargue 
    LOADED_FILES = os.path.join(script_directory,"LoadedFiles",f"{nombre_tabla}.log") # ruta del log #//root//PROCESOS//BASESCLAROCOBRANZAPRUEBAS//src//LoadedFiles//
    with open(LOADED_FILES, "r") as f: # abrir el archivo del log
        logs     = f.read().splitlines()
    logging.getLogger("user").info(f"La base '{file[22:]}' del {file[:20]} esta por cargarse") # log cargue
    logs.append(file) # Log archivo
    try:
        nombre, extension = os.path.splitext(file) # Obtener el nombre archivo y extención
        # validacion de extenciones para saber que función usar 
        if extension in [".txt", ".csv"] and nombre_tabla != 'bdd_asignacion_claro_cobranza':
            logging.getLogger("user").info(f"el archivo es un txt")
            toSqlTxt(path,nombre_tabla,file, dic_fechas,dic_formatos,separador, columnas_tabla)
        elif extension in [".xlsx", ".XLSX" ] and nombre_tabla != 'bdd_asignacion_claro_cobranza':
            logging.getLogger("user").info(f"el archivo es un excel")
            toSqlExcel(path,nombre_tabla,file, dic_fechas,dic_formatos,dic_hojas,separador, cargue_tabla, asignacion, columnas_tabla)
        elif extension in [".zip", ".ZIP"] and nombre_tabla != 'bdd_asignacion_claro_cobranza':
            logging.getLogger("user").info(f"el archivo es un zip")
            toSqlZip(path,nombre_tabla,file, dic_fechas,dic_formatos,dic_hojas,separador, nombre_archivo, cargue_tabla,asignacion, logs,columnas_tabla)
        send(f"Se acaba de cargar la base {file[22:]} de Gopass")
        logging.getLogger("user").info(f"Base '{file[22:]}' del {file[:20]} recien cargada [ToSQL]")
        
    except Exception as e:
        with open(LOADED_FILES, "w") as f:
            f.write("\n".join(logs))
        logging.getLogger("user").exception(f"Error en archivo: {file}\n{e}")
        send(f"Error cargando la base {file}: {e}")
        estado='ERROR'
        error=f"{e}"
        razon= error.replace("[^0-9a-zA-Z_]", "")
    else:
        estado='SUCCESS'
        razon=''
    finally:
        fin = time.time()
        tEjecucion=fin-ini
        razon=razon.replace("'","")
        logging.getLogger("user").info(f"Tiempo total de ejecucion: {fin-ini} de {file}")
        cdn_connection,engine,bbdd_or = mysql_connection()
        with engine.connect() as conn:
            conn.execute(text(f"""INSERT INTO {bbdd_or}.tb_loaded_files (FILE_DATE, FILE_NAME,TABLA, ESTADO,TIEMPO_EJECUCION, RAZON) VALUES  ('{file[:19]}', '{file[22:]}', '{nombre_tabla}','{estado}', '{tEjecucion}','{razon}') ON DUPLICATE KEY UPDATE  ESTADO = VALUES(ESTADO),TIEMPO_EJECUCION=VALUES(TIEMPO_EJECUCION);"""))
        logging.getLogger("user").info(f"Tiempo total de ejecucion: {fin-ini} de {file}")

def scan_folder(path,nombre_tabla,nombre_archivo,dic_fechas,dic_formatos,dic_hojas,separador, cargue_tabla, asignacion,columnas_tabla, manual):
    logging.getLogger("user").info(f"en Scan {nombre_tabla} -- {nombre_archivo}-- {path}")
    try:
        if os.path.exists(path): # validar si la ruta existe
            if  nombre_tabla == 'bdd_asignacion_claro_cobranza':
                CONNECTION = ms.varConnCol61.connect()
                SCHEMA = 'bbdd_groupcos_repositorio_claro_cobranza_v1'
                # TABLE = 'tb_bdd_asignacion_claro_cobranza'
                main = LoadDataServer(path, CONNECTION, SCHEMA, f'tb_{nombre_tabla}')._dataframe_result()
            else:
                file_to_load = Read_files_path(path,nombre_tabla,nombre_archivo, manual) # Obtener listado de los archivos a cargar 
                for file in file_to_load: # interar los archivos para el cargue
                    logging.getLogger("user").info(f"cargando {file}")
                    # validacion y extandarización para el cargue de los archivos 
                    check_and_add(path,nombre_tabla,file, dic_fechas,dic_formatos,dic_hojas,separador, cargue_tabla, asignacion, nombre_archivo, columnas_tabla) 
        else:
            send(f"No ha habido cargue de la base {nombre_tabla} del {anio}-{mes}-{dia}") 
            logging.getLogger("user").info(f"no existe la ruta")
    except Exception as e:
        print(e)
        logging.getLogger("user").exception(e)