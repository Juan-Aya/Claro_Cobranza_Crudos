# Importar las librerias neserias para el proceso
from Imports import *
from paths import *
ip = '60'

# conexión para obtener la conexion   
def mysql_connection():
    ruta_config_files = os.path.join(path_to_config,"credenciales.json")
    with open(ruta_config_files) as f:
        configuracion = json.load(f)
    credecial=configuracion["credenciales_conexion_db"]
    url       = f'mysql+pymysql://{credecial["user"]}:{quote(credecial["password"])}@{credecial["host"]}:{"3306"}/{credecial["database"]}'
    engine    = create_engine(url,pool_recycle=9600,isolation_level="AUTOCOMMIT")
    #mysql_con = engine.connect()
    #engine.dispose()
    return url, engine, credecial["database"]
