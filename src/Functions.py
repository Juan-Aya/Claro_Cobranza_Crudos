from Imports import *
from paths import *

# obtener la ruta del el archivo config de la conexion al bot telegarm
ruta_config_files = os.path.join(path_to_config, "credenciales.json")

# abrir el archivo obtener los parametros de conexión 
with open(ruta_config_files) as f:
    configuracion = json.load(f)
credecial=configuracion["credenciales_bot_tlgrm"]
key = credecial["accesos"]

# Funciones para realizar el envio del mesaje de telegram
async def send_message(msg,chat_id): 
        print(key[0])
        bot = telegram.Bot(key[0]) 
        await bot.send_message(text = msg, chat_id = chat_id)
def send(msg):
    print(f"{msg}: {key[1]}")
    asyncio.run(send_message(msg,key[1]))