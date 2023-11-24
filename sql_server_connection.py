import pyodbc 
from connection.config import config

def connection_CVN():
    cxn = config('CVN')
    server = cxn['host']
    db_name = cxn['database']
    user_name = cxn['user']
    password = '##WKU7rK5r@Gt2016' #cxn['password']

    try:
        print("conectando...")
        conexion = pyodbc.connect('DRIVER={SQL Server};SERVER=' +
                                server+';DATABASE='+db_name+';UID='+user_name+';PWD=' + password)
        print("\n"*2)
        print("conexión exitosa")
        return conexion
    except Exception as e:
        print("Ocurrió un error al conectar a SQL Server: ", e)

def connection_CVN_PROD():
    cxn = config('CVN_PROD')
    server = cxn['host']
    db_name = cxn['database']
    user_name = cxn['user']
    password = 'Quick*2023$%!' #cxn['password']

    try:
        print("conectando...")
        conexion = pyodbc.connect('DRIVER={SQL Server};SERVER=' +
                                server+';DATABASE='+db_name+';UID='+user_name+';PWD=' + password)
        print("\n"*2)
        print("conexión exitosa")
        return conexion
    except Exception as e:
        print("Ocurrió un error al conectar a SQL Server: ", e)        