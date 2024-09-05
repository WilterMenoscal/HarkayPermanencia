import psycopg2
import pandas as pd
from datetime import datetime,timedelta
import time
from psycopg2 import sql

def conexionBase():
    conexion1 = psycopg2.connect(  host='localhost', port='5432', database='mylocal', user='mylocal', password='2001')
    return conexion1

#OBTENER DF
def getdf(table,cc,fecha):
    conexion1 = conexionBase()
    sql="select fecha,hora,ins,outs from %s where id_cc=%s and fecha='%s' order by registro_id" % (table,cc,fecha)
    df = pd.read_sql_query(sql, conexion1)
    conexion1.close()
    return df

def getfechas(table, cc):
    # Establece la conexión a la base de datos
    conexion1 = conexionBase()
    # Define la consulta SQL para seleccionar las fechas únicas
    sql = f"""
        SELECT DISTINCT fecha 
        FROM {table} 
        WHERE id_cc = {cc} 
        AND TO_DATE(fecha, 'DD/MM/YYYY') >= TO_DATE('01/01/2024', 'DD/MM/YYYY') AND TO_DATE(fecha, 'DD/MM/YYYY') <= TO_DATE('16/04/2024', 'DD/MM/YYYY')
        ORDER BY fecha
    """
    # Ejecuta la consulta y carga los resultados en un DataFrame
    df = pd.read_sql_query(sql, conexion1)
    # Cierra la conexión a la base de datos
    conexion1.close()
    return df

def insert_df(df,table_name):
    try:
        # Establish connection to the database
        conexion1 = conexionBase()
        # Create a cursor object using the connection
        cursor = conexion1.cursor()     
        for index, row in df.iterrows():
            fecha = row['timestamp']
            id_cc = row['id_cc']
            ins = row['ins']
            outs = row['outs']
            # Convertir a float y manejar NaN o None
            if pd.isna(ins) or ins is None:
                ins = None  # o manejar según sea necesario
            
            if pd.isna(outs) or outs is None:
                outs = None  # o manejar según sea necesario
            
            # Define the insertion SQL statement
            insert_statement = sql.SQL("INSERT INTO {} (fecha,id_cc,ins,outs) VALUES (%s, %s, %s, %s)").format(
                sql.Identifier(table_name))

            # Execute the SQL statement with parameters
            cursor.execute(insert_statement, (fecha, id_cc, ins, outs))

        # Commit the transaction
        conexion1.commit()
        print("Data inserted successfully into table:", table_name)
    except psycopg2.Error as e:
        print("Error inserting data into PostgreSQL table:", e)
    
    finally:
        # Close cursor and connection
        # if cursor:
        #     cursor.close()
        if conexion1:
            conexion1.close()

#ACUMULAR POR INS, OUTS Y CUMIN-CUMOUT
def acumular(tinicial,tfinal,df):
    # Agrupar por 'hora' y sumar las columnas 'ins' y 'outs'
    df_grouped = df.groupby(['fecha','hora'], as_index=False).agg({'ins': 'sum', 'outs': 'sum'})
    filtro_horas = (df_grouped['hora'] >= tinicial) & (df_grouped['hora'] <= tfinal)
    df_grouped = df_grouped[filtro_horas]
    # Calcular los acumulados de 'ins' y 'outs'
    df_grouped['ins_acumulados'] = df_grouped.groupby('fecha')['ins'].cumsum()
    df_grouped['outs_acumulados'] = df_grouped.groupby('fecha')['outs'].cumsum()
    df_grouped['cumin-cumout'] = df_grouped['ins_acumulados'] - df_grouped['outs_acumulados']
    print(df_grouped)
    df_grouped['timestamp'] = pd.to_datetime(df_grouped['fecha'] + ' ' + df_grouped['hora'], format='%d/%m/%Y %H:%M:%S')
    
    return df_grouped

def principal(table,cc,fechas,tinicial,tfinal):
    for i in range(0,len(fechas)):
        #OBTENER DATAFRAME SEGUN CC Y DIA
        df=getdf(table,cc,fechas[i])
        df_grouped1=acumular(tinicial,tfinal,df)
        df_grouped1['id_cc']=cc
        print(df_grouped1)
        df_grouped1['timestamp'] = pd.to_datetime(df_grouped1['timestamp'])
        # Sumar 30 días a la columna 'fecha'
        df_grouped1['timestamp'] = df_grouped1['timestamp'] + pd.Timedelta(days=122) #122 
        #Convertir la columna 'fecha' a tipo datetime
        df_grouped1['timestamp'] = pd.to_datetime(df_grouped1['timestamp'])
        print(df_grouped1)
        table_name2='insouts7'
        insert_df(df_grouped1,table_name2)


start_time = time.time()
table='ingreso_persona_local2'
cc=[3]
tinicial='09:00:00'
tfinal='22:00:00'
# Convertir las cadenas de tiempo a objetos datetime
for i in cc:
    fechas = getfechas(table, i)
    lista_fechas = []
    for fecha in fechas['fecha']:
        lista_fechas.append(fecha)  
    # Usar en caso de querer una o algunas fechas en especifico
    # lista_fechas=['02/01/2024','03/01/2024','04/01/2024','05/01/2024']
    principal(table,i,lista_fechas,tinicial,tfinal)

end_time = time.time()
execution_time = end_time - start_time

# Imprimir el tiempo de ejecución
print(f"Tiempo de ejecución: {execution_time} segundos") 