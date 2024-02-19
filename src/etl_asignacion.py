from Imports import *
from Connections import LoadDataframePandas as ld


class ScriptEmail:
    
    def __init__(self):
        pass
    
    def _clean_email(email):
        def _not_email(email):
            if '@' in email :
                result = email
            else :
                result = '-'
            return result 

        email = _not_email(str(email))
        email = re.sub("^[a-z0-9]+[\._]?[a-z0-9]+[@]\w+[.]\w{2,3}$" , '', email)
        return email

class LoadDataServer:
    RENAME_COLS = {
                    'Número de Cliente':'CLIENTE'	,
                    '[Account.AccountCode?]':'CUENTA'	,
                    'CRM Origen':'CRM_ORIGEN'	,
                    'Edad de Deuda':'EDAD_DEUDA'	,
                    '[PotencialMark?]':'POTENCIAL_MARK'	,
                    '[PrePotencialMark?]':'PRE_PORTENCIAL_MARK'	,
                    '[WriteOffMark?]':'WRITE_OFF_MARK'	,
                    'Monto inicial':'MONTO_INCIAL'	,
                    '[ModInitCta?]':'MOD_INIT_CTA'	,
                    '[DeudaRealCuenta?]':'DEUDA_REAL_CUENTA'	,
                    '[BillCycleName?]':'BILL_CYCLE_NAME'	,
                    'Nombre Campaña':'NOMBRE_CAMPANA'	,
                    '[DebtAgeInicial?]':'DEBT_AGE_INICIAL'	,
                    'Nombre Casa de Cobro':'NOMBRE_CASA_DE_COBRO'	,
                    'Fecha de Asignacion':'FECHA_DE_ASIGNACION'	,
                    'Deuda Gestionable':'DEUDA_GESTIONABLE'	,
                    'Dirección Completa':'DIRECCION_COMPLETA'	,
                    'Fecha Final ':'FECHA_FINAL'	,
                    'Segmento':'SEGMENTO'	,
                    '[Documento?]':'DOCUMENTO'	,
                    '[AccStsName?]':'ACC_STS_NAME'	,
                    'Ciudad':'CIUDAD'	,
                    '[InboxName?]':'INBOX_NAME'	,
                    'Nombre del Cliente':'NOMBRE_DEL_CLIENTE'	,
                    'Id de Ejecucion':'ID_DE_EJECUCION'	,
                    'Fecha de Vencimiento':'FECHA_DE_VENCIMIENTO'	,
                    'Numero Referencia de Pago':'NUMERO_REFERENCIA_DE_PAGO'	,
                    'MIN':'MIN'	,
                    'Plan':'PLAN'	,
                    'Cuotas Aceleradas':'CUOTAS_ACELERADAS'	,
                    'Fecha de Aceleracion':'FECHA_DE_ACELERACION'	,
                    'Valor Acelerado':'VALOR_ACELERADO'	,
                    'Intereses Contingentes':'INTERESES_CONTINGENTES'	,
                    'Intereses Corrientes Facturados':'INTERESES_CORRIENTES_FACTURADOS'	,
                    'Intereses por mora facturados':'INTERESES_POR_MORA_FACTURADOS'	,
                    'Cuotas Facturadas':'CUOTAS_FACTURADAS'	,
                    'Iva Intereses Contigentes Facturado':'IVA_INTERESES_CONTINGENTES_FACTURADOS'	,
                    'Iva Intereses Corrientes Facturados':'IVA_INTERESES_CORRIENTES_FACTURADOS'	,
                    'Iva Intereses por Mora Facturado':'IVA_INTERESES_POR_MORA_FACTURADO'	,
                    'Precio Subscripcion':'PRECIO_SUBSCRIPCION'	,
                    'Código de proceso':'CODIGO_DE_PROCESO'	,
                    '[CustomerTypeId?]':'CUSTOMER_TYPE_ID'	,
                    '[RefinanciedMark?]':'REFINANCIED_MARK'	,
                    '[Discount?]':'DISCOUNT'	,
                    '[Permanencia?]':'PERMANENCIA'	,
                    '[DeudaSinPermanencia?]':'DEUDA_SIN_PERMANENCIA'    ,
                    'Telefono 1' : 'TELEFONO_1' ,
                    'Telefono 2' : 'TELEFONO_2' ,
                    'Telefono 3' : 'TELEFONO_3' ,
                    'Telefono 4' : 'TELEFONO_4' ,
                    'Email' : 'EMAIL' ,
                    '[ActivesLines?]' : 'ACTIVES_LINES'
                    }
    
    
    DTYPES_COLS = {
                    'Número de Cliente':'object'	,
                    '[Account.AccountCode?]':'object'	,
                    'CRM Origen':'category'	,
                    'Edad de Deuda':'category'	,
                    '[PotencialMark?]':'category'	,
                    '[PrePotencialMark?]':'category'	,
                    '[WriteOffMark?]':'category'	,
                    'Monto inicial':'object'	,
                    '[ModInitCta?]':'object'	,
                    '[DeudaRealCuenta?]':'object'	,
                    '[BillCycleName?]':'category'	,
                    'Nombre Campaña':'category'	,
                    '[DebtAgeInicial?]':'category'	,
                    'Nombre Casa de Cobro':'category'	,
                    'Fecha de Asignacion':'object'	,
                    'Deuda Gestionable':'object'	,
                    'Dirección Completa':'object'	,
                    'Fecha Final ':'object'	,
                    'Segmento':'object'	,
                    '[Documento?]':'object'	,
                    '[AccStsName?]':'category'	,
                    'Ciudad':'category'	,
                    '[InboxName?]':'category'	,
                    'Nombre del Cliente':'category'	,
                    'Id de Ejecucion':'category'	,
                    'Fecha de Vencimiento':'object'	,
                    'Numero Referencia de Pago':'object'	,
                    'MIN':'object'	,
                    'Plan':'category'	,
                    'Cuotas Aceleradas':'category'	,
                    'Fecha de Aceleracion':'object'	,
                    'Valor Acelerado':'object'	,
                    'Intereses Contingentes':'category'	,
                    'Intereses Corrientes Facturados':'category'	,
                    'Intereses por mora facturados':'category'	,
                    'Cuotas Facturadas':'object'	,
                    'Iva Intereses Contigentes Facturado':'object'	,
                    'Iva Intereses Corrientes Facturados':'object'	,
                    'Iva Intereses por Mora Facturado':'object'	,
                    'Precio Subscripcion':'object'	,
                    'Código de proceso':'category'	,
                    '[CustomerTypeId?]':'category'	,
                    '[RefinanciedMark?]':'category'	,
                    '[Discount?]':'object'	,
                    '[Permanencia?]':'category'	,
                    'Telefono 1':'object'   ,
                    'Telefono 2':'object'   ,
                    'Telefono 3':'object'   ,
                    'Telefono 4':'object'   ,
                    'Email': 'object' ,
                    '[ActivesLines?]' : 'object'
                    }
    
    
    
    def __init__(self, path, connection, schema, table):
        self.path = path
        self.connection = connection
        self.schema = schema
        self.table = table

    def _get_today_path(self):
        today = (dt.today() - timedelta(days=2)).strftime('%Y//%m//%d')
        return os.path.join(self.path, today)

    def _scan_folder_documents(self):
        path = self._get_today_path()

        if not os.path.exists(path):
            print(f'Path {path} does not exist.')
            return None

        files = [os.path.join(path, f) for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))]
        if not files:
            print(f'No files found in {path}.')
            return None

        return max(files, key=os.path.getmtime)

    def _read_excel(self, path):
        try:
            with open(path, 'r', encoding='utf-8') as f:
                contents = f.read().replace('\0', '') 
                df = pd.read_csv(StringIO(contents), delimiter=';', on_bad_lines='skip', index_col=False, chunksize=150_000, encoding='utf-8', dtype=self.DTYPES_COLS , skip_blank_lines=False , engine='python' , quoting=3 )
            return df
        except pd.errors.ParserError as e:
            print(f"Error reading {path}: {e}")
            return None

    def _filter_new_accounts(self):
        path = self._scan_folder_documents()
        if not path:
            return

        anho_mes = dt.today().strftime('%Y_%m')
        query = f'select CUENTA from {self.schema}.{self.table} where ANHO_MES = "{anho_mes}"'

        df = self._read_excel(path)
        if df is None:
            return
        

        df_server = set(pd.read_sql_query(query, self.connection)['CUENTA'])

        df_list = [chunk[~chunk['[Account.AccountCode?]'].isin(df_server)] for chunk in df]
        print(df_list)
        df = pd.concat(df_list)
        df.rename(columns=self.RENAME_COLS, inplace=True)
        df['CLIENTE'] = df['CLIENTE'].apply(lambda x : re.sub('[^0-9]','',x))
        df['NOMBRE_BASE'] = str(path).split('\\')[len(str(path).split('\\'))-1]
        df['ANHO_MES'] = dt.today().strftime('%Y_%m')
        df['FECHA_ASIGNACION'] = dt.today().strftime('%Y-%m-%d')
        df['EMAIL_MOD1'] = df['EMAIL'].apply(ScriptEmail._clean_email)
        
        return df
    
    def _dataframe_result(self):
        
        
        def _franjas(crm_origen , potencial_mark , pre_portencial_mark ,write_off_mark , deb_age_inicial , refinancied_mark , nombre_campana ):
            
            refinancied_mark = str(refinancied_mark)
            
            if potencial_mark == 'N' and pre_portencial_mark == 'N' and write_off_mark == 'Y' and refinancied_mark == 'nan':
                franja = 'CASTIGO'
            elif crm_origen == 'ASCARD' and potencial_mark == 'N' and pre_portencial_mark == 'N' and write_off_mark == 'N' and refinancied_mark == 'Y':
                franja = 'REFINANCIADO'
            elif crm_origen == 'BSCS' and potencial_mark == 'Y' and pre_portencial_mark == 'N' and write_off_mark == 'N':
                franja = 'POTENCIAL'
            elif crm_origen == 'BSCS' and potencial_mark == 'N' and pre_portencial_mark == 'Y' and write_off_mark == 'N' and nombre_campana in ['PrePotencial Convergente Masivo_2','PrePotencial Convergente Pyme_2']:
                franja = 'PREPOTENCIAL_ESPECIAL'
            elif crm_origen == 'BSCS' and potencial_mark == 'N' and pre_portencial_mark == 'Y' and write_off_mark == 'N' and nombre_campana not in ['PrePotencial Convergente Masivo_2','PrePotencial Convergente Pyme_2']:
                franja = 'PREPOTENCIAL_ORDINARIO'
            elif crm_origen == 'RR' and potencial_mark == 'Y' and pre_portencial_mark == 'N' and write_off_mark == 'N':
                franja = 'CHURN'
            elif crm_origen == 'SGA' and potencial_mark == 'Y' and pre_portencial_mark == 'N' and write_off_mark == 'N':
                franja = 'CHURNFO'
            elif crm_origen in['RR'] and potencial_mark == 'N' and pre_portencial_mark == 'Y' and write_off_mark == 'N':
                franja = 'PRECHURN'
            elif crm_origen in['SGA'] and potencial_mark == 'N' and pre_portencial_mark == 'Y' and write_off_mark == 'N':
                franja = 'PRECHURN_SGA'
            elif crm_origen in['BSCS','RR','SGA'] and potencial_mark == 'N' and pre_portencial_mark == 'N' and write_off_mark == 'N' and refinancied_mark == 'Y':
                franja = 'POTENCIAL_CASTIGO'
            elif crm_origen in['BSCS','RR','SGA'] and potencial_mark == 'N' and pre_portencial_mark == 'N' and write_off_mark == 'Y' and refinancied_mark == 'Y':
                franja = 'POTENCIAL_CASTIGO'
            elif crm_origen in['SGA'] and potencial_mark == 'N' and pre_portencial_mark == 'Y' and write_off_mark == 'N' and refinancied_mark == 'nan':
                franja = 'PRECHURN_FO'
            elif crm_origen == 'ASCARD' and potencial_mark == 'Y' and pre_portencial_mark == 'N' and write_off_mark == 'N' and refinancied_mark == 'nan':
                franja = 'PROVISION'        
            elif crm_origen == 'ASCARD' and potencial_mark == 'N' and pre_portencial_mark == 'Y' and write_off_mark == 'N' and refinancied_mark == 'nan':
                franja = 'PREPROVISION'        
            elif potencial_mark == 'N' and pre_portencial_mark == 'N' and write_off_mark == 'N' and deb_age_inicial == '0' and refinancied_mark == 'nan':
                franja = 'EDAD_0'
            elif potencial_mark == 'N' and pre_portencial_mark == 'N' and write_off_mark == 'N' and deb_age_inicial == '30' and refinancied_mark == 'nan':
                franja = 'EDAD_30'
            elif potencial_mark == 'N' and pre_portencial_mark == 'N' and write_off_mark == 'N' and deb_age_inicial == '60' and refinancied_mark == 'nan':
                franja = 'EDAD_60'
            elif potencial_mark == 'N' and pre_portencial_mark == 'N' and write_off_mark == 'N' and deb_age_inicial == '90' and refinancied_mark == 'nan':
                franja = 'EDAD_90'
            elif potencial_mark == 'N' and pre_portencial_mark == 'N' and write_off_mark == 'N' and deb_age_inicial == '120' and refinancied_mark == 'nan':
                franja = 'EDAD_120'
            elif potencial_mark == 'N' and pre_portencial_mark == 'N' and write_off_mark == 'N' and deb_age_inicial == '150' and refinancied_mark == 'nan':
                franja = 'EDAD_150'
            elif potencial_mark == 'N' and pre_portencial_mark == 'N' and write_off_mark == 'N' and deb_age_inicial == '180' and refinancied_mark == 'nan':
                franja = 'EDAD_180'
            elif potencial_mark == 'N' and pre_portencial_mark == 'N' and write_off_mark == 'N' and deb_age_inicial == '210' and refinancied_mark == 'nan':
                franja = 'EDAD_210'
            else :
                franja = 'VALIDAR'
                
            return franja
        
        def _split_country(country):
            country = str(country).split('/')[0]
            return country.strip()
        
        df = self._filter_new_accounts()
        # df['CIUDADES'] = df['CIUDAD'].apply(_split_country)
        df['FECHA_DE_ASIGNACION'] = df['FECHA_DE_ASIGNACION'].apply(pd.to_datetime , format='%d/%m/%Y %I:%M %p', errors='ignore')
        df['FECHA_FINAL'] = df['FECHA_FINAL'].apply(pd.to_datetime , format='%d/%m/%Y %I:%M %p', errors='ignore')
        df['FECHA_DE_VENCIMIENTO'] = df['FECHA_DE_VENCIMIENTO'].apply(pd.to_datetime , format='%d/%m/%Y' , errors='ignore')
        df['FECHA_DE_ACELERACION'] = df['FECHA_DE_ACELERACION'].apply(pd.to_datetime , format='%d/%m/%Y' , errors='ignore')
        df['NOMBRE_FRANJA'] = df.apply(lambda x : _franjas(x['CRM_ORIGEN'] , x['POTENCIAL_MARK'] , x['PRE_PORTENCIAL_MARK'] , x['WRITE_OFF_MARK'] , x['DEBT_AGE_INICIAL'] , x['REFINANCIED_MARK'] , x['NOMBRE_CAMPANA']) , axis=1)
        
        query_server = f'select * from {self.schema}.{self.table} limit 1'
        df_server = pd.read_sql_query(query_server, self.connection)
        ld.funMainLoadDataServerNotTruncate(self.connection, self.table, self.schema, df, df_server)
        
        return print(df)

