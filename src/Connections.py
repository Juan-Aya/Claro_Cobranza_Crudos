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


##  Crea clase de conexion a MySql Server  ##

class MySqlConnections : 

    
    def funMySqlConnection(varDbms , varSchema , varServer , varPort , varUser , varPass ):    
        varMySqlQuery = f'{varDbms}://{varUser}:{varPass}@{varServer}:{varPort}/{varSchema}'   
        return create_engine(varMySqlQuery)
    
    ## 1. Objeto de conexion a mysql servidor 172.17.8.61
    ruta_config_files = os.path.join(path_to_config,"credenciales.json")
    with open(ruta_config_files) as f:
        configuracion = json.load(f)
    credecial=configuracion["credenciales_conexion_db"]
    varDbms = 'mysql'
    varSchema = 'bbdd_config'
    varServer = credecial["host"]
    varPort = '3306'
    varUser = credecial["user"]
    varPass = credecial["password"]
    
    varConnCol61 = funMySqlConnection( varDbms , varSchema , varServer , varPort , varUser , varPass )

class LoadDataframePandas:
    
    
    def __init__(self , varSchema , varTable , objMySqlConnection , varDataframeResult , varNombreBaseOperacion):
        self.varSchema = varSchema
        self.varTable = varTable
        self.objMySqlConnection = objMySqlConnection
        self.varDataframeResult = varDataframeResult
        self.varNombreBaseOperacion = varNombreBaseOperacion
    
    def funMainLoadDataServerNotTruncate(objMySqlConnection , varTable , varSchema , varDataframeFinal , varDataframeServer ):
        
            
        
        def funInspectSchema(objMySqlConnection , varTable , varSchema):
            
            
            varMySqlInspect = sqa.inspect(objMySqlConnection)
            varInspectResult = varMySqlInspect.has_table(varTable ,schema= varSchema )
            return varInspectResult
        
        
        def funAddColumns(objMySqlConnection , varInspectServer , varDataframeFinal , varDataframeServer, varSchema , varTable):
            
            
            def funCompareColumns(varDataframeFinal , varDataframeServer):
                varDataframeFinalColumns = varDataframeFinal.columns.to_list()
                varDataframeServerColumns = varDataframeServer.columns.to_list()
                varListDifferenceColumns = set(varDataframeFinalColumns).difference(set(varDataframeServerColumns))
                return varListDifferenceColumns
            
            
            def funAddColumsServer(objMySqlConnection , varListColumnsToAdd , varSchema , varTable ):
                
                def funExecuteQueryAdd(objMySqlConnection , varListAlterTableColumns):
                    try:
                        objMySqlConnection.execute(varListAlterTableColumns)
                    except:
                        print('The column {0} is duplicate.'.format(varListAlterTableColumns))
                
                
                varListAlterTableColumns = ['ALTER TABLE `{0}`.`{1}` ADD COLUMN `{2}` VARCHAR(64)'.format(varSchema , varTable , i ) for i in varListColumnsToAdd]
                varExecuteAlterTableColumns = [funExecuteQueryAdd(objMySqlConnection , i) for i in varListAlterTableColumns]
                return 
                
            
            varListColumnsToAdd : list = funCompareColumns(varDataframeFinal , varDataframeServer)
            varListAlterTableColumns : list = funAddColumsServer(objMySqlConnection , varListColumnsToAdd , varSchema , varTable)
            
            
            return
                
        
        def funLoadDataframeServer(objMySqlConnection , varDataframeResult , varTable , varSchema ): 
            
            
            
            def funInsertOnDuplicate(table, conn, keys, data_iter):
                insert_stmt = insert(table.table).values(list(data_iter)) 
                on_duplicate_key_stmt = insert_stmt.on_duplicate_key_update(insert_stmt.inserted)
                conn.execute(on_duplicate_key_stmt) 
                
            varDataframeResult.to_sql( varTable ,objMySqlConnection , varSchema , if_exists = 'append', index = False ,  method = funInsertOnDuplicate , chunksize=50000)
            
            
        def funLoadDataframeServerIndex(objMySqlConnection , varDataframeResult , varTable , varSchema ): 
            
            def funInsertOnDuplicate(table, conn, keys, data_iter):
                insert_stmt = insert(table.table).values(list(data_iter)) 
                on_duplicate_key_stmt = insert_stmt.on_duplicate_key_ignore(insert_stmt.inserted)
                conn.execute(on_duplicate_key_stmt) 
                
            varDataframeResult.to_sql( varTable ,objMySqlConnection , varSchema , if_exists = 'append', index = True , method = funInsertOnDuplicate , chunksize=50000)
            
        
        def funDeleteDataServer(objMySqlConnection , varTable , varSchema ):
            varDeleteQuery = 'delete from `{0}`.`{1}`'.format(varSchema , varTable)
            try:
                objMySqlConnection.execute(varDeleteQuery)
            except:
                objMySqlConnection = objMySqlConnection 
                
        
        def funDeleteDataServerNombreBaseOperacion(objMySqlConnection , varTable , varSchema , varNombreBaseOperacion):
            varDeleteQuery = 'delete from `{0}`.`{1}` where nombre_base_operacion = "{2}"'.format(varSchema , varTable , varNombreBaseOperacion)
            try:
                objMySqlConnection.execute(varDeleteQuery)
            except:
                objMySqlConnection = objMySqlConnection 
            
        
        def funTruncateDataServer(objMySqlConnection , varTable , varSchema ):
            varDeleteQuery = 'truncate table `{0}`.`{1}`'.format(varSchema , varTable)
            try:
                objMySqlConnection.execute(varDeleteQuery)
            except:
                objMySqlConnection = objMySqlConnection 
                
        
        varInspectServer = funInspectSchema(objMySqlConnection , varTable , varSchema)
        
        
        funAddColumns(objMySqlConnection , varInspectServer , varDataframeFinal , varDataframeServer, varSchema , varTable)
        
        
        funLoadDataframeServer(objMySqlConnection , varDataframeFinal , varTable , varSchema )