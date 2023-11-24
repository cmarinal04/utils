import pandas as pd
import numpy as np
from connection.sql_server_connection import connection_CVN_PROD as sqlCVN 
from utils.send_email import send_email_smtp


# Definir funciones para aplicar las condiciones en las columnas
def seleccionar_tipo_documento(row):
    return row['Tipo de Documento_Lleno'] if pd.notnull(row['Tipo de Documento_Lleno']) else row['Tipo de Documento']

def seleccionar_numero_preimpreso(row):
    return row['Número de Preimpreso_Lleno'] if pd.notnull(row['Número de Preimpreso_Lleno']) else row['Número de Preimpreso']

def seleccionar_numero_aceptacion(row):
    return row['numero aceptación_Lleno'] if pd.notnull(row['numero aceptación_Lleno']) else row['numero aceptación']


def create_file(df_data, file_name):
    name_file = f'C:\inetpub\wwwroot\OnlineConsulta\ArchivosExcel\Especializado\{file_name}.xlsx'
    update_creation_file(id)
    df = pd.DataFrame(df_data)
    max_rows = 1000000
    shell_total = (len(df) - 1) // max_rows + 1
    writer = pd.ExcelWriter(name_file, engine='xlsxwriter')
    for hoja_numero in range(shell_total):
        inicio = hoja_numero * max_rows
        fin = min((hoja_numero + 1) * max_rows, len(df))
        df_parte = df.iloc[inicio:fin]
        df_parte.to_excel(writer, sheet_name=f'Hoja{hoja_numero + 1}', index=False)
    # Guardar el archivo Excel
    writer.close()
    print("Archivo Excel creado con éxito.")
    update_ended_file(id, file_name)
    return name_file

def read_unidad_medida(resultado_df):
    cnx = sqlCVN()
    query = '''SELECT * FROM RegistroImportacion.UnidadMedida WITH(NOLOCK)'''
    df_unidad_medida = pd.read_sql(query, cnx)
    resultado_df['ItemUnidadCodigo'] = resultado_df['ItemUnidadCodigo'].apply(lambda x: 0 if pd.isnull(x) else int(x))
    resultado_df = pd.merge(resultado_df,df_unidad_medida,left_on='ItemUnidadCodigo',right_on='UnidadMedidaId', how='left')
    resultado_df['ItemUnidad'] = resultado_df.apply(   lambda row: row['ItemUnidad'] if pd.isnull(row['UnidadMedidaDescripcion']) else f"{row['UnidadMedidaDescripcion']} - {str(row['UnidadMedidaId'])}",    axis=1)
    return resultado_df


def update_creation_file(id):
    cnx = sqlCVN()
    query = f''' UPDATE [Especializado].[GeneracionEspecializado] 
                SET GeneracionEspecializadoFechaInicio = SYSDATETIMEOFFSET() AT TIME ZONE 'SA Pacific Standard Time'
                WHERE [GeneracionEspecializadoId] = {id} '''
    cursor = cnx.cursor()
    cursor.execute(query)
    cnx.commit()
    cursor.close()
    cnx.close()
    

def update_status_file(id, file_name):
    cnx = sqlCVN()
    query = f''' 	UPDATE Especializado.GeneracionEspecializado
                    SET EstadoGeneracionEspecializadoId = 5
                        , GeneracionEspecializadoFechaFin = SYSDATETIMEOFFSET() AT TIME ZONE 'SA Pacific Standard Time'
                    WHERE GeneracionEspecializadoId = {id} '''
    cursor = cnx.cursor()
    cursor.execute(query)
    cnx.commit()
    cursor.close()
    cnx.close()


def update_ended_file(id, file_name):
    cnx = sqlCVN()
    query = f''' 	UPDATE Especializado.GeneracionEspecializado
                    SET EstadoGeneracionEspecializadoId = 3
                        , GeneracionEspecializadoFechaFin = SYSDATETIMEOFFSET() AT TIME ZONE 'SA Pacific Standard Time'
                    WHERE GeneracionEspecializadoId = {id} '''
    cursor = cnx.cursor()
    cursor.execute(query)
    cnx.commit()
    cursor.close()
    cnx.close()


def read_ProcesamientoEspecializado_Inicializar(PaqueteSISSiD: int):
    stored_procedure_name = 'Especializado.ProcesamientoEspecializado_Inicializar'
    cnxn  = sqlCVN()
    cursor = cnxn.cursor()
    # Ejecutar el procedimiento almacenado
    cursor.execute(f"EXEC {stored_procedure_name} {PaqueteSISSiD}")
    ProcesamientoEspecializadoId = cursor.fetchone()
    cursor.commit()
    cursor.close()
    cnxn.close()
    return ProcesamientoEspecializadoId
    

def read_GeneracionEspecializado_Obtener(ProcesamientoEspecializadoId):
    stored_procedure_name = 'Especializado.GeneracionEspecializado_Obtener'
    cnxn  = sqlCVN()
    # Obtener los resultados en un DataFrame de Pandas
    df = pd.read_sql_query(f"EXEC {stored_procedure_name} {ProcesamientoEspecializadoId[0]}", cnxn)
    cnxn.close()
    return df
    
def update_GeneracionEspecializado_Obtener(GeneracionEspecializadoId):
    sql_update = (f"""
                    UPDATE Especializado.GeneracionEspecializado
                    SET GeneracionEspecializadoFechaInicio = SYSDATETIMEOFFSET() AT TIME ZONE 'SA Pacific Standard Time'
                    WHERE GeneracionEspecializadoId = {GeneracionEspecializadoId};
                 """)
    cnxn = sqlCVN()
    with cnxn.cursor() as cursor:
        cursor.execute(sql_update)
        cnxn.commit()
    cnxn.close()


def update_item_participacion_total(row):
    if row['ItemTotalCalculado'] == 0 or row['SubPartidaTotalCalculado'] == 0:
        return 0
    else:
        return (row['ItemTotalCalculado'] / row['SubPartidaTotalCalculado']) * 100


def update_item_participacion(row):
    if row['ItemCantidad_x'] == 0 or row['SubPartidaCantidadCalculada'] == 0:
        return 0
    else:
        return (row['ItemCantidad_x'] / row['SubPartidaCantidadCalculada']) * 100


def update_LicenciaPosicionDeclaracion(df_LicenciaPosicionDeclaracion : pd, df_DeclaracionCVNOnLine : pd):
    ids_concatenados = ', '.join(df_LicenciaPosicionDeclaracion['Nº de Licencia'].astype(str))
    clausula_where = f"WHERE Codigo IN ('C462') AND Descripcion IN ({', '.join([f'{val}' for val in ids_concatenados.split(', ')])})"
    v_sql = f"""
            SELECT * FROM Regimenes {clausula_where}
             """
    cnxn  = sqlCVN()
    df = pd.read_sql(v_sql, cnxn)

    v_sql_subpartida = f"""
                        SELECT Subpartida.[SubPartidaNumero], Registro.RegistroNumeroLicencia
                        FROM RegistroImportacion.Registro WITH (NOLOCK)
                        INNER JOIN RegistroImportacion.Subpartida WITH (NOLOCK)
                            ON Subpartida.RegistroId = Registro.RegistroId
                        WHERE CONVERT(VARCHAR,CONVERT(DECIMAL,Subpartida.[SubPartidaNumero])) LIKE '8706%';
                        """
    df_subpartida = pd.read_sql(v_sql_subpartida, cnxn)
    #REVISAR, FALTA ACÁ
    pass


def read_DetalleItemDeDeclaracionCVNOnLineBase(df_sin_duplicados: pd):
    numeros = df_sin_duplicados['Nº de Licencia'][df_sin_duplicados['Nº de Licencia'].astype(str).str.isnumeric()]
    ids_concatenados = ', '.join(numeros)
    cnxn  = sqlCVN()
    v_sql = f"""
            SELECT 
          Id
          ,Consecutivo
          ,RegistroNumeroLicencia AS RegistroNumeroFormulario
		  ,[SubPartidaNumero]
		  ,[ItemCantidad]
		  ,ItemUnidad
		  ,ItemUnidadCodigo
		  ,[ItemParticipacionCantidad]
		  ,[ItemPrecio]
		  ,[ItemTotalCalculado]
		  ,[ItemParticipacionTotal]
		  ,[ItemDescripcion]
		  ,[SubPartidaTotalCalculado]
		  ,[SubPartidaCantidadCalculada]
		  ,[ItemTotal]
		  ,RegistroId
		  ,SubPartidaId
          ,RegistroFecha
		  ,ItemId
		FROM 
		(
			SELECT 
			  ROW_NUMBER() OVER (ORDER BY RegistroNumeroLicencia, SubPartidaNumero, SubPartidaNumeroItem, ItemNumero ) AS [Id]
              ,0 as [Consecutivo]
              ,Registro.RegistroNumeroLicencia
			  ,[SubPartidaNumero]
			  ,[ItemCantidad]
			  ,ItemUnidad
			  ,ItemUnidadCodigo
			  ,[ItemParticipacionCantidad]
			  ,[ItemPrecio]
			  ,[ItemTotalCalculado]
			  ,[ItemParticipacionTotal]
			  ,[ItemDescripcion]
			  ,[SubPartidaTotalCalculado]
			  ,[SubPartidaCantidadCalculada]
			  ,[ItemTotal]
			  ,Registro.RegistroId
			  ,SubPartida.SubPartidaId
              ,Registro.RegistroFecha
			  ,Item.ItemId
			  ,SubPartidaNumeroItem
			  ,ItemNumero
			FROM
			(
				SELECT  RegistroId
					, RegistroNumeroLicencia
                    , RegistroFecha
				FROM RegistroImportacion.Registro WITH (NOLOCK)
				WHERE RegistroId > 0 and RegistroNumeroLicencia IN ({ids_concatenados})
			) AS Registro
			INNER JOIN RegistroImportacion.SubPartida WITH (NOLOCK) 
				ON Registro.RegistroId = SubPartida.RegistroId 
			INNER JOIN RegistroImportacion.Item WITH (NOLOCK) 
				ON SubPartida.SubPartidaId = Item.SubPartidaId
		) Data
		ORDER BY RegistroNumeroLicencia
			, SubPartidaNumero
			, SubPartidaNumeroItem
			, ItemNumero
            """
    df = pd.read_sql(v_sql, cnxn)
    return df


def update_datos_1(df_DeclaracionCVNOnLine, df_DetalleItemDeDeclaracionCVNOnLine):

    # Filtrar los datos según la condición WHERE en CTE_FilaDeclaracion
    df_FilaDeclaracion = df_DeclaracionCVNOnLine[df_DeclaracionCVNOnLine['Nº de Licencia'] > '0']
    
    # Agregar una columna con el número de fila por partición en CTE_FilaDeclaracion
    order_columns = ['Año', 'MesNumero', 'Día', 'Nº de Licencia', 'Posición Arancelaria', 'Número de Preimpreso', 'numero aceptación', 'Número de Declaracion de Importación']
    df_FilaDeclaracion['Row'] = df_FilaDeclaracion.sort_values(by=order_columns).groupby(['Nº de Licencia', 'Posición Arancelaria', 'Año', 'Mes']).cumcount() + 1

    # Crear CTE_DetalleDeclaracion
    df_CTE_DetalleDeclaracion = df_FilaDeclaracion.groupby(['Nº de Licencia', 'Posición Arancelaria', 'Año', 'Mes']).agg(TotalItem=('Nº de Licencia', 'count'),Periodo=('Año', 'first'),MesNombre=('Mes', 'first'),MaximoRow=('Row', 'max')).reset_index()

    # Crear CTE_DetalleItem
    df_DetalleItem = df_DetalleItemDeDeclaracionCVNOnLine.groupby(['RegistroNumeroFormulario', 'SubPartidaNumero']).agg(TotalItems=pd.NamedAgg(column='SubPartidaNumero', aggfunc='count')).reset_index()

    # Crear CTE_DetalleDeclaracionFaltante
    df_DetalleDeclaracionFaltante = pd.merge(df_CTE_DetalleDeclaracion, df_DetalleItem, left_on=['Nº de Licencia', 'Posición Arancelaria'], right_on=['RegistroNumeroFormulario', 'SubPartidaNumero'],    how='inner')
    df_DetalleDeclaracionFaltante = df_DetalleDeclaracionFaltante[df_DetalleDeclaracionFaltante['TotalItem'] < df_DetalleDeclaracionFaltante['TotalItems']]
    df_DetalleDeclaracionFaltante['TotalItem'] = df_DetalleDeclaracionFaltante['TotalItems'].sub(df_DetalleDeclaracionFaltante['TotalItem'])
    
    df_DetalleDeclaracionFaltanteAjustado = pd.DataFrame()

    for value in df_DetalleDeclaracionFaltante['Nº de Licencia']:
        df_aux = df_DetalleDeclaracionFaltante[df_DetalleDeclaracionFaltante['Nº de Licencia'] == value]
        df_duplicado = df_aux.loc[df_aux.index.repeat(df_aux['TotalItem'])].reset_index(drop=True)
        df_aux_duplicado = df_duplicado[['Nº de Licencia','Posición Arancelaria','Periodo','Mes','TotalItem','MaximoRow']]
        df_DetalleDeclaracionFaltanteAjustado = pd.concat([df_DetalleDeclaracionFaltanteAjustado, df_aux_duplicado], ignore_index=False)

    # Unir con CTE_FilaDeclaracion para obtener el DataFrame final
    df_FilaDeclaracion = pd.concat([df_DetalleDeclaracionFaltanteAjustado, df_FilaDeclaracion], ignore_index=False)

    # Devuelve el DataFrame final
    return(df_FilaDeclaracion)


def update_datos_2(df_DeclaracionCVNOnLine, df_DetalleItemDeDeclaracionCVNOnLine):
    CTE_DetalleItem = df_DetalleItemDeDeclaracionCVNOnLine.groupby(['RegistroNumeroFormulario', 'SubPartidaNumero']).agg(TotalItems=('RegistroNumeroFormulario', 'count'),MaximoId=('Id', 'max')).reset_index()
    CTE_DetalleDeclaracion = df_DeclaracionCVNOnLine.groupby(['Nº de Licencia', 'Posición Arancelaria', 'Año', 'MesNumero']).size().reset_index(name='TotalItem')
    df_CTE_DetalleItemFaltante = pd.merge(CTE_DetalleItem,CTE_DetalleDeclaracion,left_on=['RegistroNumeroFormulario', 'SubPartidaNumero'],right_on=['Nº de Licencia', 'Posición Arancelaria'],how='inner')
    #df_CTE_DetalleItemFaltante['TotalItems'] = df_CTE_DetalleItemFaltante['TotalItem'].sub(df_CTE_DetalleItemFaltante['TotalItems'])
    df_DetalleDeclaracionFaltanteAjustado = pd.DataFrame()

    for index, row in df_CTE_DetalleItemFaltante[['Nº de Licencia', 'Posición Arancelaria']].iterrows():
        value1 = row['Nº de Licencia']
        value2 = row['Posición Arancelaria']
        df_aux = df_CTE_DetalleItemFaltante[(df_CTE_DetalleItemFaltante['Nº de Licencia'] == value1) & (df_CTE_DetalleItemFaltante['Posición Arancelaria'] == value2)]
        df_duplicado = df_aux.loc[df_aux.index.repeat(df_aux['TotalItems'])].reset_index(drop=True)
        df_aux_duplicado = df_duplicado[['Nº de Licencia','Posición Arancelaria','Año','MesNumero','TotalItems','MaximoId']]
        df_DetalleDeclaracionFaltanteAjustado = pd.concat([df_DetalleDeclaracionFaltanteAjustado, df_aux_duplicado], ignore_index=False)

    return (df_DetalleDeclaracionFaltanteAjustado)


def insert_resultado(df_DeclaracionCVNOnLine, df_DetalleItemDeDeclaracionCVNOnLine, df_SubpartidaDeclaracionCVNOnLine,df_ConsecutivoDeclaracionCVNOnLine):
    merged_df = pd.merge(df_ConsecutivoDeclaracionCVNOnLine, df_DetalleItemDeDeclaracionCVNOnLine, left_on=['MaximoId', 'Nº de Licencia', 'Posición Arancelaria'],right_on=['Id', 'RegistroNumeroFormulario', 'SubPartidaNumero'],how='left')

    merged_df = pd.merge(merged_df, df_SubpartidaDeclaracionCVNOnLine,left_on=['Posición Arancelaria', 'Nº de Licencia', 'Año', 'MesNumero'],right_on=['Posición Arancelaria', 'Nº de Licencia', 'Año', 'MesNumero'],how='left')    

    merged_df = pd.merge(merged_df, df_DeclaracionCVNOnLine,left_on=[ 'Nº de Licencia', 'Posición Arancelaria', 'Año', 'MesNumero','Consecutivo_x'],right_on=['Nº de Licencia', 'Posición Arancelaria', 'Año', 'MesNumero','Consecutivo_x'],how='left')

    merged_df['Cantidad_x'] = pd.to_numeric(merged_df['Cantidad_x'], errors='coerce')
    merged_df['ItemParticipacionCantidad'] = pd.to_numeric(merged_df['ItemParticipacionCantidad'], errors='coerce')
    merged_df['Flete USD_x'] = pd.to_numeric(merged_df['Flete USD_x'], errors='coerce')
    merged_df['ItemParticipacionTotal'] = pd.to_numeric(merged_df['ItemParticipacionTotal'], errors='coerce')
    merged_df['Seguro USD_x'] = pd.to_numeric(merged_df['Seguro USD_x'], errors='coerce')

    resultado_df = pd.DataFrame({
    'RegistroNumeroFormulario': merged_df['RegistroNumeroFormulario'].combine_first(merged_df['Nº de Licencia']),
    'SubPartidaNumero': merged_df['SubPartidaNumero'].combine_first(merged_df['Posición Arancelaria']),
    'ItemUnidad': merged_df['ItemUnidad'].combine_first(pd.Series(['NO APLICA'] * len(merged_df))),
    'ItemUnidadCodigo': merged_df['ItemUnidadCodigo'].fillna(''),
    'ItemCantidad': pd.to_numeric(merged_df['ItemCantidad_x'], errors='coerce'),
    'ItemParticipacionCantidad': pd.to_numeric(merged_df['ItemParticipacionCantidad'], errors='coerce'),
    'ItemPrecio': pd.to_numeric(merged_df['ItemPrecio'], errors='coerce'),
    'ItemTotalCalculado': pd.to_numeric(merged_df['ItemTotalCalculado'], errors='coerce'),
    'ItemParticipacionTotal': pd.to_numeric(merged_df['ItemParticipacionTotal'], errors='coerce'),
    'SubPartidaTotalCalculado': pd.to_numeric(merged_df['SubPartidaTotalCalculado'], errors='coerce'),
    'SubPartidaCantidadCalculada': pd.to_numeric(merged_df['SubPartidaCantidadCalculada'], errors='coerce'),
    'Año': merged_df['Año'],
    'Mes': merged_df['Mes'],
    'Día': merged_df['Día'],
    'Tipo de Documento': merged_df['Tipo de Documento'],
    'Número de Preimpreso': merged_df['Número de Preimpreso'],
    'numero aceptación': merged_df['numero aceptación'],
    'Número de Declaracion de Importación': merged_df['Número de Declaracion de Importación'],
    'Banco': merged_df['Banco'],
    'Tipo de Declaración': merged_df['Tipo de Declaración'],
    'Oficina de Aduana': merged_df['Oficina de Aduana'],
    'Régimen': merged_df['Régimen'],
    'NIT agente aduanero': merged_df['NIT agente aduanero'],
    'Agente Aduanero': merged_df['Agente Aduanero'],
    'NIT del Importador': merged_df['NIT del Importador'],
    'Razón Social del Importador': merged_df['Razón Social del Importador'],
    'Dpto# del Importador': merged_df['Dpto# del Importador'],
    'Dirección Importador': merged_df['Dirección Importador'],
    'Tel# del Importador': merged_df['Tel# del Importador'],
    'Designación de la Mercancía': merged_df['Designación de la Mercancía'],
    'Descripción de la Mercancía': merged_df['Descripción de la Mercancía'],
    'Descripción de la Mercancía 2': merged_df['Descripción de la Mercancía 2'],
    'Cantidad': merged_df['Cantidad_x'],
    #'Nueva Cantidad': (merged_df['CantidadTotal_x'] * merged_df['ItemParticipacionCantidad']) / 100,
    'Nueva Cantidad': (merged_df['Cantidad_x'] * merged_df['ItemParticipacionCantidad']) / 100,
    'Nueva Cantidad Ejecución': merged_df['ItemCantidad_x'] * np.where(merged_df['SubPartidaTotalCalculado'] == 0,   0, np.where( (merged_df['valor fob US$_x'] / merged_df['SubPartidaTotalCalculado']) * 100.0 > 100.0, 1.0, (merged_df['valor fob US$_x'] / merged_df['SubPartidaTotalCalculado']) * 1.0)),
    'Unidad Comercial': merged_df['Unidad Comercial'],
    'Número de Bultos': merged_df['Número de Bultos'],
    'Embalaje': merged_df['Embalaje'],
    'Peso Neto Kgs#': merged_df['Peso Neto Kgs#'],
    'Peso Bruto Kgs#': merged_df['Peso Bruto Kgs#'],
    'Acuerdo': merged_df['Acuerdo'],
    'Depósito': merged_df['Depósito'],
    'País de Origen': merged_df['País de Origen'],
    'País Procedencia': merged_df['País Procedencia'],
    'País de Compra': merged_df['País de Compra'],
    'Dpto# de Ingreso': merged_df['Dpto# de Ingreso'],
    'Proveedor': merged_df['Proveedor'],
    'Ciudad/País del Proveedor': merged_df['Ciudad/País del Proveedor'],
    'Dirección del Proveedor': merged_df['Dirección del Proveedor'],
    'Contacto del Proveedor': merged_df['Contacto del Proveedor'],
    'Nº de Manifiesto': merged_df['Nº de Manifiesto'],
    'Fecha del Manifiesto': merged_df['Fecha del Manifiesto'],
    'Documento de Transporte': merged_df['Documento de Transporte'],
    'Transporte': merged_df['Transporte'],
    'Transportador': merged_df['Transportador'],
    'Tasa de Cambio COP$': merged_df['Tasa de Cambio COP$'],
    'Forma de Pago': merged_df['Forma de Pago'],
    'Porcentaje Arancel': merged_df['Porcentaje Arancel'],
    'Porcentaje IVA': merged_df['Porcentaje IVA'],
    'Arancel Pagado COP': merged_df['Arancel Pagado COP'],
    'IVA Pagado COP': merged_df['IVA Pagado COP'],
    'Valor Fletes y Seguros USD': merged_df['Valor Fletes y Seguros USD_y'],
    'Nuevo Valor Fletes y Seguros USD': np.where(((merged_df['Flete USD_x'] * merged_df['ItemParticipacionTotal']) / 100).isnull() | (((merged_df['Flete USD_x'] * merged_df['ItemParticipacionTotal']) / 100) == 0), merged_df['Valor Fletes y Seguros USD_x'], ((merged_df['Flete USD_x'] * merged_df['ItemParticipacionTotal']) / 100)),
    'Flete USD': merged_df['Flete USD_x'],
    'Nuevo Flete USD': np.where(((merged_df['Flete USD_x'] * merged_df['ItemParticipacionTotal']) / 100).isnull() | (((merged_df['Flete USD_x'] * merged_df['ItemParticipacionTotal']) / 100) == 0), merged_df['Flete USD_x'], ((merged_df['Flete USD_x'] * merged_df['ItemParticipacionTotal']) / 100)),
    'Seguro USD': merged_df['Seguro USD_x'],
    'Nuevo Seguro USD': np.where(((merged_df['Seguro USD_x'] * merged_df['ItemParticipacionTotal']) / 100).isnull() | (((merged_df['Seguro USD_x'] * merged_df['ItemParticipacionTotal']) / 100) == 0), merged_df['Seguro USD_x'], ((merged_df['Seguro USD_x'] * merged_df['ItemParticipacionTotal']) / 100)),
    'Porcentaje Salvaguardia': merged_df['Porcentaje Salvaguardia'],
    'Vlr# Total Salvaguardia COP': merged_df['Vlr# Total Salvaguardia COP'],
    'Porcentaje de Derechos Compensatorios': merged_df['Porcentaje de Derechos Compensatorios'],
    'Base Derechos Compensatorios USD': merged_df['Base Derechos Compensatorios USD'],
    'Vlr Total Derechos Compensatorios COP': merged_df['Vlr Total Derechos Compensatorios COP'],
    'Porcentaje Dumping': merged_df['Porcentaje Dumping'],
    'Base Dumping USD': merged_df['Base Dumping USD'],
    'Vlr# Total Dumping COP': merged_df['Vlr# Total Dumping COP'],
    'valor fob US$': merged_df['valor fob US$_x'],
    'Precio FOB USD Unitario': pd.to_numeric(merged_df['Precio FOB USD Unitario'].str.replace(',', ''), errors='coerce'),
    'Nuevo Valor fob US$': (merged_df['valor fob US$_y'] * merged_df['ItemParticipacionTotal']) / 100,
    'valor FOB COP$': merged_df['valor CIF COP$'],
    'valor cif US $': (merged_df['valor cif US $_x'] * merged_df['ItemParticipacionTotal']) / 100,
    'valor CIF COP$': merged_df['valor CIF COP$'],
    'PorcentajeEjecucion': np.where(merged_df['SubPartidaTotalCalculado'] == 0, 0, np.where((merged_df['valor fob US$_x'] / merged_df['SubPartidaTotalCalculado']) * 100.0 > 100.0, 100.0, (merged_df['valor fob US$_x'] / merged_df['SubPartidaTotalCalculado']) * 100.0)),
    'Nuevo Fob Calculado': merged_df['ItemPrecio'] * ((merged_df['Cantidad_x'] * merged_df['ItemParticipacionCantidad']) / 100),
    'Nuevo Fob Calculado Ejecución': merged_df['ItemPrecio'] * (merged_df['ItemCantidad_x'] * np.where(merged_df['SubPartidaTotalCalculado'] == 0, 0, np.where((merged_df['valor fob US$_x'] / merged_df['SubPartidaTotalCalculado']) * 100.0 > 100.0, 1.0, (merged_df['valor fob US$_x'] / merged_df['SubPartidaTotalCalculado']) * 1.0))),
    'Variación Fob Calculado': np.where((merged_df['ItemPrecio'] * ((merged_df['Cantidad_x'] * merged_df['ItemParticipacionCantidad']) / 100)) == 0, 0.0, (((merged_df['ItemPrecio'] * ((merged_df['Cantidad_x'] * merged_df['ItemParticipacionCantidad']) / 100)) - ((merged_df['valor fob US$_x'] * merged_df['ItemParticipacionTotal']) / 100)) / (merged_df['ItemPrecio'] * ((merged_df['Cantidad_x'] * merged_df['ItemParticipacionCantidad']) / 100)))),
    'Variación Fob Calculado Ejecución': np.where((merged_df['ItemPrecio'] * (merged_df['ItemCantidad_x'] * np.where(merged_df['SubPartidaTotalCalculado'] == 0, 0, np.where((merged_df['valor fob US$_x'] / merged_df['SubPartidaTotalCalculado']) * 100.0 > 100.0, 1.0, (merged_df['valor fob US$_x'] / merged_df['SubPartidaTotalCalculado']) * 1.0)))) == 0, 0.0, (((merged_df['ItemPrecio'] * (merged_df['ItemCantidad_x'] * np.where(merged_df['SubPartidaTotalCalculado'] == 0, 0, np.where((merged_df['valor fob US$_x'] / merged_df['SubPartidaTotalCalculado']) * 100.0 > 100.0, 1.0, (merged_df['valor fob US$_x'] / merged_df['SubPartidaTotalCalculado']) * 1.0)))) - ((merged_df['valor fob US$_x'] * merged_df['ItemParticipacionTotal']) / 100)) / (merged_df['ItemPrecio'] * (merged_df['ItemCantidad_x'] * np.where(merged_df['SubPartidaTotalCalculado'] == 0, 0, np.where((merged_df['valor fob US$_x'] / merged_df['SubPartidaTotalCalculado']) * 100.0 > 100.0, 1.0, (merged_df['valor fob US$_x'] / merged_df['SubPartidaTotalCalculado']) * 1.0)))))),
    'RegistroId': merged_df['RegistroId'],
    'SubPartidaId': merged_df['SubPartidaId'],
    'ItemId': merged_df['ItemId']
    })
    breakpoint()
    resultado_df = read_unidad_medida(resultado_df)
    columnas_combinacion = ['RegistroNumeroFormulario', 'SubPartidaNumero', 'Año', 'Mes']
    breakpoint()
    resultado_df = resultado_df.groupby(['RegistroNumeroFormulario', 'SubPartidaNumero', 'Año', 'Mes']).apply(lambda group: group.fillna(method='ffill')).reset_index(drop=True)
    breakpoint()
    df_filtrado = resultado_df[(resultado_df['Tipo de Documento'].str.len() > 0) | (resultado_df['Número de Preimpreso'].str.len() > 0)]
    columnas_seleccionadas = ['RegistroNumeroFormulario', 'SubPartidaNumero', 'Año', 'Mes', 'Día','Tipo de Documento', 'Número de Preimpreso', 'numero aceptación', 'Número de Declaracion de Importación', 'Banco', 'Tipo de Declaración', 'Oficina de Aduana', 'Régimen', 'NIT agente aduanero',
    'Agente Aduanero', 'NIT del Importador', 'Razón Social del Importador', 'Dpto# del Importador', 'Dirección Importador', 'Tel# del Importador', 'Designación de la Mercancía', 'Unidad Comercial',
    'Embalaje', 'Acuerdo', 'Depósito', 'País de Origen', 'País Procedencia', 'País de Compra', 'Dpto# de Ingreso', 'Proveedor', 'Ciudad/País del Proveedor', 'Dirección del Proveedor',
    'Contacto del Proveedor', 'Nº de Manifiesto', 'Fecha del Manifiesto', 'Documento de Transporte', 'Transporte', 'Transportador', 'Forma de Pago', 'Porcentaje Arancel', 'Porcentaje IVA', 'Arancel Pagado COP', 'IVA Pagado COP' ]
    df_resultado_final_lleno = df_filtrado[columnas_seleccionadas]

    columnas_combinacion = ['RegistroNumeroFormulario', 'SubPartidaNumero', 'Año', 'Mes']

    # Realizar la combinación con left join
    df_combinado = pd.merge(resultado_df, df_resultado_final_lleno, on=columnas_combinacion, how='left', suffixes=('', '_Lleno') )
    df_combinado['Tipo de Documento'] = df_combinado.apply(seleccionar_tipo_documento, axis=1)
    df_combinado['Número de Preimpreso'] = df_combinado.apply(seleccionar_numero_preimpreso, axis=1)
    df_combinado['numero aceptación'] = df_combinado.apply(seleccionar_numero_aceptacion, axis=1)

    breakpoint()
    return resultado_df


def read_firs_sql_data(query: str):
    cnxn  = sqlCVN()
    query = query.replace('Mes.Nombre As [Mes]','Mes.Nombre As [Mes], Mes.Numero AS MesNumero')
    with open("SQL\send_mails\detailed\\001_Query_Inicial_Online.sql", 'r', encoding='utf-8') as myfile:
        data = myfile.read()
        data = str.format(data, query)

    df_DeclaracionCVNOnLine = pd.read_sql(data, cnxn)
    df_sin_duplicados = df_DeclaracionCVNOnLine[df_DeclaracionCVNOnLine['NIT del Importador'] != ''].drop_duplicates(subset='Nº de Licencia') 
    df_DetalleItemDeDeclaracionCVNOnLineBase = read_DetalleItemDeDeclaracionCVNOnLineBase(df_sin_duplicados)
    df_DetalleItemDeDeclaracionCVNOnLine = df_DetalleItemDeDeclaracionCVNOnLineBase.sort_values(by='Id')
    df_agregado = df_DeclaracionCVNOnLine[['Id', 'Nº de Licencia', 'Posición Arancelaria', 'Año', 'MesNumero', 'Número de Preimpreso']].copy()
    df_agregado = df_agregado.rename(columns={'Año': 'Periodo', 'MesNumero': 'Mes'})
    df_agregado['Número de Preimpreso'] = df_agregado['Número de Preimpreso'].apply(lambda x: '9999999999999999' if x == '' else x)
    df_agregado['Consecutivo'] = df_agregado.groupby(['Nº de Licencia', 'Posición Arancelaria', 'Periodo', 'Mes']).cumcount() + 1
    df_DeclaracionCVNOnLine = pd.merge(df_DeclaracionCVNOnLine, df_agregado[['Id', 'Consecutivo']], left_on='Id', right_on='Id', how='inner')
    df_DeclaracionCVNOnLine['Consecutivo_x'] = df_DeclaracionCVNOnLine['Consecutivo_y']


    df_LicenciaPosicionDeclaracion = df_DeclaracionCVNOnLine[df_DeclaracionCVNOnLine['Posición Arancelaria'].str.contains('8702|8704')]

    if not (df_LicenciaPosicionDeclaracion == 0).all().all():
        update_LicenciaPosicionDeclaracion(df_LicenciaPosicionDeclaracion, df_DeclaracionCVNOnLine)
        
        
        #FALTA ACÁ
    
    resultado = df_DetalleItemDeDeclaracionCVNOnLine.groupby(['RegistroNumeroFormulario', 'SubPartidaNumero']).agg({'ItemCantidad': 'sum', 'ItemTotal': 'sum'}).reset_index()
    df_resultado = pd.merge(df_DetalleItemDeDeclaracionCVNOnLine, resultado, on=['RegistroNumeroFormulario', 'SubPartidaNumero'], how='inner')
    df_resultado['ItemCantidad_x'] = df_resultado['ItemCantidad_y']
    df_resultado['ItemTotal_x'] = df_resultado['ItemTotal_y']
    df_resultado = df_resultado.drop(columns=['ItemCantidad_y'])
    df_resultado = df_resultado.drop(columns=['ItemTotal_y'])
    df_DetalleItemDeDeclaracionCVNOnLine = df_resultado

    df_DetalleItemDeDeclaracionCVNOnLine['ItemParticipacionCantidad'] = df_DetalleItemDeDeclaracionCVNOnLine.apply(lambda row: update_item_participacion(row), axis=1)
    df_DetalleItemDeDeclaracionCVNOnLine['ItemTotalCalculado'] = df_DetalleItemDeDeclaracionCVNOnLine['ItemPrecio'] * df_DetalleItemDeDeclaracionCVNOnLine['ItemCantidad_x']
    df_DetalleItemDeDeclaracionCVNOnLine['ItemParticipacionTotal'] = df_DetalleItemDeDeclaracionCVNOnLine.apply(lambda row: update_item_participacion_total(row), axis=1)

    df_SubpartidaDeclaracionCVNOnLine = df_DeclaracionCVNOnLine.groupby(['Nº de Licencia', 'Posición Arancelaria', 'Año', 'MesNumero']).agg({'Cantidad': 'sum','Valor Fletes y Seguros USD': 'sum', 'Flete USD': 'sum', 'Seguro USD': 'sum', 'valor fob US$': 'sum', 'valor cif US $': 'sum'}).reset_index()
    
    # ACA VIENE EL FILTRO DE Actualizar Datos 1
    update_Data_1 = 0
    if(update_Data_1 == 1):

        df_Resultado_ajuste_datos_1 = update_datos_1(df_DeclaracionCVNOnLine, df_DetalleItemDeDeclaracionCVNOnLine)

     # ACA VIENE EL FILTRO DE Actualizar Datos 2
    update_Data_2 = 1
    if(update_Data_2 == 1):
        df_DetalleDeclaracionFaltanteAjustado_datos2 = update_datos_2(df_DeclaracionCVNOnLine, df_DetalleItemDeDeclaracionCVNOnLine)
    
        df_ConsecutivoDeclaracionCVNOnLine = pd.DataFrame()
        df_ConsecutivoDeclaracionCVNOnLine = df_DetalleDeclaracionFaltanteAjustado_datos2['MaximoId']
        df_ConsecutivoDeclaracionCVNOnLine = df_DetalleDeclaracionFaltanteAjustado_datos2.copy()
        df_ConsecutivoDeclaracionCVNOnLine['Consecutivo'] = df_ConsecutivoDeclaracionCVNOnLine.groupby(['Nº de Licencia', 'Posición Arancelaria', 'Año', 'MesNumero'])['MaximoId'].cumcount() + 1

        df_DetalleItemDeDeclaracionCVNOnLine['Detalle'] = (df_DetalleItemDeDeclaracionCVNOnLine['RegistroNumeroFormulario'].astype(str) + '|' + df_DetalleItemDeDeclaracionCVNOnLine['SubPartidaNumero'].astype(str) + '|' + '|' + df_DetalleItemDeDeclaracionCVNOnLine['Consecutivo'].astype(str))
    
    insert_resultado(df_DeclaracionCVNOnLine, df_DetalleItemDeDeclaracionCVNOnLine, df_SubpartidaDeclaracionCVNOnLine,df_ConsecutivoDeclaracionCVNOnLine)
    



