import psycopg2
import logging
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime,timedelta
import numpy as np
from matplotlib.ticker import MaxNLocator
import math
import time
import random
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

# def getfechas(table, cc):
#     # Establece la conexión a la base de datos
#     conexion1 = conexionBase()

#     # Define la consulta SQL para seleccionar las fechas únicas
#     sql = f"SELECT DISTINCT fecha FROM {table} WHERE id_cc={cc} ORDER BY fecha"

#     # Ejecuta la consulta y carga los resultados en un DataFrame
#     df = pd.read_sql_query(sql, conexion1)

#     # Cierra la conexión a la base de datos
#     conexion1.close()

#     # Devuelve el DataFrame con las fechas únicas
#     return df

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
    # Devuelve el DataFrame con las fechas únicas
    return df

def insert_df(df,table_name):
    try:
        # Establish connection to the database
        conexion1 = conexionBase()
        # Create a cursor object using the connection
        cursor = conexion1.cursor()     
        for index, row in df.iterrows():
            fecha = row['fecha']
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



def insert_df_into_table(df_acum ,df, table_name,table_name1):
    try:
        # Establish connection to the database
        conexion1 = conexionBase()
        # Create a cursor object using the connection
        cursor = conexion1.cursor()
        cursor1 = conexion1.cursor()

        # Iterate over each row in the DataFrame and insert into the table
        for index, row in df_acum.iterrows():
            fecha = row['fecha']
            id_cc = row['id_cc']
            duracion = row['duracion']
            personas = row['personas']
# Revisa si hay algún valor extremadamente grande

            if pd.isna(duracion) or duracion is None:
                duracion = None  
            if pd.isna(personas) or personas is None:
                personas = None 
            if pd.isna(fecha) or fecha is None:
                personas = None 
            if pd.isna(id_cc) or id_cc is None:
                personas = None 
            
            # Define the insertion SQL statement
            insert_statement = sql.SQL("INSERT INTO {} (fecha, id_cc,duracion,personas) VALUES (%s,  %s, %s, %s)").format(
                sql.Identifier(table_name1))

            # Execute the SQL statement with parameters
            cursor.execute(insert_statement, (fecha, id_cc,duracion,personas))
     
        for index, row in df.iterrows():
            fecha = row['hora']
            id_cc = row['id_cc']
            cumincumout = row['cumin-cumout_avg']
            duracionh = row['duracionH_avg']
            salidas= row['outs_avg']
            # Convertir a float y manejar NaN o None
            if pd.isna(cumincumout) or cumincumout is None:
                cumincumout = None  # o manejar según sea necesario
            
            if pd.isna(duracionh) or duracionh is None:
                duracionh = None  # o manejar según sea necesario
            
            # Convertir personas a entero y manejar NaN o None
            if pd.isna(salidas) or salidas is None:
                salidas = None  # o manejar según sea necesario
            
            # Define the insertion SQL statement
            insert_statement = sql.SQL("INSERT INTO {} (fecha, id_cc, cumincumout, duracionh, salidas) VALUES (%s, %s, %s, %s, %s)").format(
                sql.Identifier(table_name))

            # Execute the SQL statement with parameters
            cursor1.execute(insert_statement, (fecha, id_cc, cumincumout, duracionh, salidas))

        # Commit the transaction
        conexion1.commit()
        print("Data inserted successfully into table:", table_name)

    except psycopg2.Error as e:
        print("Error inserting data into PostgreSQL table:", e)
    
    finally:
        # Close cursor and connection
        # if cursor:
        #     cursor.close()
        if cursor1:
            cursor1.close()
        if conexion1:
            conexion1.close()


#ACUMULAR POR INS, OUTS Y CUMIN-CUMOUT
def acumular(tinicial,tfinal,df):
    # Agrupar por 'hora' y sumar las columnas 'ins' y 'outs'
    df_grouped = df.groupby(['fecha','hora'], as_index=False).agg({'ins': 'sum', 'outs': 'sum'})
    filtro_horas = (df_grouped['hora'] >= tinicial) & (df_grouped['hora'] <= tfinal)
    df_grouped = df_grouped[filtro_horas]
    # Calcular los acumulados de 'ins' y 'outs' por fecha
    df_grouped['ins_acumulados'] = df_grouped.groupby('fecha')['ins'].cumsum()
    df_grouped['outs_acumulados'] = df_grouped.groupby('fecha')['outs'].cumsum()
    df_grouped['cumin-cumout'] = df_grouped['ins_acumulados'] - df_grouped['outs_acumulados']
    df_grouped2 = df_grouped.copy(deep=True)
    df_grouped2['timestamp'] = pd.to_datetime(df_grouped2['fecha'] + ' ' + df_grouped2['hora'], format='%d/%m/%Y %H:%M:%S')
    val=0
    if df_grouped2.empty:
        val=0
    else:
        val=1
    indexes=[]
    for index, row in df_grouped.iterrows():
        indexes.append(index)

    for i in range(0,len(indexes)):
        n=random.randint(60,100)
        if  df_grouped.at[indexes[i], 'cumin-cumout'] < 0:
            deficit = -df_grouped.at[indexes[i], 'cumin-cumout']
            # Calcular 3/4 del valor de deficit
            deficit1 = int((3 / 4) * deficit)
            # Calcular 1/4 del valor de deficit
            deficit2 = int((1 / 4) * deficit)
            if i>=n:
                df_grouped.at[indexes[i-n], 'ins'] =  df_grouped.at[indexes[i-n], 'ins'] + deficit
                df_grouped['ins_acumulados'] = df_grouped.groupby('fecha')['ins'].cumsum()
                df_grouped['cumin-cumout'] = df_grouped['ins_acumulados'] - df_grouped['outs_acumulados']
            else:
                df_grouped.at[indexes[i], 'ins'] =  df_grouped.at[indexes[i], 'ins'] + deficit1
                df_grouped.at[indexes[i], 'outs'] =  df_grouped.at[indexes[i], 'outs'] - deficit2
                df_grouped['ins_acumulados'] = df_grouped.groupby('fecha')['ins'].cumsum()
                df_grouped['outs_acumulados'] = df_grouped.groupby('fecha')['outs'].cumsum()
                df_grouped['cumin-cumout'] = df_grouped['ins_acumulados'] - df_grouped['outs_acumulados']

    df_grouped['timestamp'] = pd.to_datetime(df_grouped['fecha'] + ' ' + df_grouped['hora'], format='%d/%m/%Y %H:%M:%S')
    
    return df_grouped,val

#FILTRADO DE DATOS DE INS Y OUTS
def insouts(df,fechas1):
    entradas={}
    salidas={}    
    df['datetime'] = pd.to_datetime(df['fecha'] + ' ' + df['hora'], format='%d/%m/%Y %H:%M:%S')
    for idx in df.index:
        date = df.loc[idx, 'datetime']
        hour = df.loc[idx, 'hora']
        ingresos = df.loc[idx, 'ins']
        salida = df.loc[idx, 'outs']
        if ingresos != 0:
            if idx == df.index[0]:
                # Para el primer índice del DataFrame
                entradas[idx] = (date,hour, ingresos)
            else:
                entradas[idx] = (date,hour, ingresos)
        if salida != 0:
            if idx == df.index[0]:
                a=0
            else:
                salidas[idx-1] = (date,hour, salida)

    suma_total = sum(value[2] for key, value in entradas.items())
    print("Ingresos:", suma_total)
    suma_total = sum(value[2] for key, value in salidas.items())
    print("Salidas:", suma_total)

    # Convertir los diccionarios a DataFrames para facilitar la graficación
    df_entradas = pd.DataFrame.from_dict(entradas, orient='index', columns=['datetime','hora', 'ins'])
    df_salidas = pd.DataFrame.from_dict(salidas, orient='index', columns=['datetime','hora', 'outs'])

# Ordenar los DataFrames por 'datetime'
    df_entradas = df_entradas.sort_values(by='datetime')
    df_salidas = df_salidas.sort_values(by='datetime')
    return df_entradas,df_salidas

def duration(entradas_df, salidas_df):
    # Convertir las horas a objetos datetime.time una vez
    entradas_df['hora'] = pd.to_datetime(entradas_df['hora'], format='%H:%M:%S').dt.time
    salidas_df['hora'] = pd.to_datetime(salidas_df['hora'], format='%H:%M:%S').dt.time
    resultado = pd.DataFrame(columns=['duracion', 'personas','fecha'])
    resultado3 = pd.DataFrame(columns=['duracion', 'personas', 'hora_salida','fecha'])
    # Crear una copia de las entradas para no modificar el DataFrame original
    for idx_salida, row_salida in salidas_df.iterrows():
        hora_salida = row_salida['hora']
        personas_salidas = row_salida['outs']
        personas_restantes_salir = personas_salidas

        # Filtrar entradas hasta la hora de salida
        entradas_filtradas = entradas_df[entradas_df['hora'] <= hora_salida]

        for idx_entrada, row_entrada in entradas_filtradas.iterrows():
            hora_entrada = row_entrada['hora']
            personas_entradas = row_entrada['ins']
            
            if personas_entradas > 0:
                if personas_restantes_salir <= personas_entradas:
                    # Restar las personas que salieron de las entradas disponibles
                    entradas_df.at[idx_entrada, 'ins'] -= personas_restantes_salir
                    # Calcular la duración y guardar el resultado
                    duracion = (datetime.combine(datetime.today(), hora_salida) - datetime.combine(datetime.today(), hora_entrada)).total_seconds() / 60
                    resultado = resultado._append({'duracion': duracion, 'personas': personas_restantes_salir,'fecha': row_salida['datetime']}, ignore_index=True)
                    resultado3 = resultado3._append({'duracion': duracion, 'personas': personas_restantes_salir, 'hora_salida': hora_salida,'fecha': row_salida['datetime']}, ignore_index=True)
                    personas_restantes_salir = 0
                    break
                else:
                    # Caso especial: más salidas que entradas, procesar parcialmente
                    personas_restantes_salir -= personas_entradas
                    duracion = (datetime.combine(datetime.today(), hora_salida) - datetime.combine(datetime.today(), hora_entrada)).total_seconds() / 60
                    resultado = resultado._append({'duracion': duracion, 'personas': personas_entradas,'fecha': row_salida['datetime']}, ignore_index=True)
                    resultado3 = resultado3._append({'duracion': duracion, 'personas': personas_entradas, 'hora_salida': hora_salida,'fecha': row_salida['datetime']}, ignore_index=True)
                    entradas_df.at[idx_entrada, 'ins'] = 0
    

    resultado['fecha'] = resultado['fecha'].dt.strftime('%Y-%m-%d %H:%M:%S')

    # Convertir la columna 'fecha' a datetime 
    #resultado['fecha'] = pd.to_datetime(resultado['fecha'])
    # Crear una nueva columna que combine la fecha y la hora
    resultado['fecha'] = pd.to_datetime(resultado['fecha'])
    # Extraer fechas únicas
    fechas_unicas = resultado['fecha'].dt.date.unique()
    # Convertir resultados a listas para mayor claridad
    lista_fechas_unicas = fechas_unicas.tolist()
    acumulados_df = resultado.groupby(['duracion'], as_index=False)['personas'].sum()

    # Añadir una columna 'fecha' en acumulados_df con valores de lista_fechas_unicas
    num_repeats = (len(acumulados_df) // len(lista_fechas_unicas)) + 1
    acumulados_df['fecha'] = (lista_fechas_unicas * num_repeats)[:len(acumulados_df)]

    return lista_fechas_unicas,acumulados_df, resultado3

def filtrado(acumulados):
    # Filtrar duraciones mayores que 0
    acumulados_filtrados = acumulados[acumulados['duracion'] > 0]
    
    # Calcular el producto total y la suma de personas
    acumulados_filtrados['product'] = acumulados_filtrados['duracion'] * acumulados_filtrados['personas']
    sum_product = np.sum(acumulados_filtrados['product'])
    sum_people = np.sum(acumulados_filtrados['personas'])

    # Imprimir resultados
    print('total minutos/personas:', sum_product / sum_people)
    print('Total personas:', sum_people)
    print('Producto total:', sum_product)
    return acumulados_filtrados

#GRAFICO FECHA- SALIDAS DURACION X PERSONA
def salidasduxpe(resultados2):
    # Calcular 'duracionxpersona'
    resultados2['duracionxpersona'] = resultados2['duracion'] * resultados2['personas']
    
    # Agrupar por 'hora_salida' y sumar los valores de 'duracionxpersona' y 'personas'
    df_agrupado = resultados2.groupby(['fecha', 'hora_salida'], as_index=False).agg({
        'duracionxpersona': 'sum',
        'personas': 'sum'
    })
    
    # Calcular 'duracionH'
    df_agrupado['duracionH'] = df_agrupado['duracionxpersona'] / df_agrupado['personas']
    
    # Mostrar resultados
    df_agrupado['hora_salida'] = df_agrupado['hora_salida'].astype(str)
    return df_agrupado

def calcular_punto_medio(intervalo):
    # Obtén los límites del intervalo
    limite_superior = intervalo.right
    return limite_superior

def principal(table,cc,fechas,fechas1,tinicial,tfinal):
    for i in range(0,len(fechas)):
        df=getdf(table,cc,fechas[i])
        df_grouped1,val=acumular(tinicial,tfinal,df)
        if val == 1:
            entradas,salidas=insouts(df_grouped1,fechas1[i])
            day,acumulados,resultados2=duration(entradas,salidas)
            df_acum=filtrado(acumulados)
            df_agrupado=salidasduxpe(resultados2)
            
            df_grouped1['fecha'] = df_grouped1['timestamp']            # Unir df_grouped1 y df_acum usando 'fecha'
            df_final = pd.merge(df_grouped1[['fecha', 'cumin-cumout','ins','outs']],df_agrupado[['fecha','duracionH']], on='fecha', how='outer')
            # Unir el resultado anterior con df_agrupado usando 'fecha'
            df_final['num'] = 1
            table_name = 'freq_datos1'
            table_name1='histogram1'
            table_name2='insouts1'
            # Convertir la columna 'fecha' a tipo datetime
            df_final['fecha'] = pd.to_datetime(df_final['fecha'])
            df_final['id_cc'] = cc
            date = pd.to_datetime(fechas[i], format='%d/%m/%Y')
            date_str = date.strftime('%Y-%m-%d')
            # Concatenar la fecha y la hora con un espacio
            datetime_str = date_str + ' ' + tinicial
            tinicial_time = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')
            tatras_time = tinicial_time - timedelta(hours=1)
            day3 = tatras_time.strftime('%Y-%m-%d %H:%M:%S')
            nueva_fila = {
                'fecha': day3,  # Puedes especificar el valor que desees
                'cumin-cumout': 0,   # Valores iniciales para las columnas sumadas
                'duracionH': 0,
                'outs': 0,
                'ins':0,
                'num': 1,  # Para evitar la división por cero en los promedios
                'id_cc': cc
            }

            # Convertir el diccionario a un DataFrame de una sola fila
            nueva_fila_df = pd.DataFrame([nueva_fila])
            # Agregar la nueva fila al DataFrame existente
            groupeddf = df_final._append(nueva_fila_df, ignore_index=True)
            groupeddf['fecha'] = pd.to_datetime(groupeddf['fecha'], format='%Y-%m-%d %H:%M:%S')
            groupeddf = groupeddf.sort_values(by='fecha')
            insert_df(groupeddf,table_name2)
            
            # Extraer la hora de la columna 'fecha' y crear una nueva columna 'hora'
            df_final['hora'] = df_final['fecha'].dt.floor('H')
            #GRAFICO POR MINUTOS
            # Agrupar por la nueva columna 'hora'
            grouped = df_final.groupby('hora').agg({
                'cumin-cumout': 'sum',  # Sumar 'cumin-cumout'
                'duracionH': 'sum',     # Sumar 'duracionH'
                'outs': 'sum',          # Sumar 'outs'
                'num': 'count'        # Contar los registros para promediar más tarde
            }).reset_index()

            # Calcular el promedio de cada columna sumada
            grouped['cumin-cumout_avg'] = grouped['cumin-cumout'] / grouped['num']
            grouped['duracionH_avg'] = grouped['duracionH'] / grouped['num']
            grouped['outs_avg'] = grouped['outs'] / grouped['num']
            grouped['id_cc'] = cc
            # Mostrar el resultado

            max_duracion = int(df_acum['duracion'].max())

            # Calcula el número de intervalos
            num_intervals = min(24, max_duracion)

            # Define los límites de los intervalos
            limites_intervals = np.linspace(0, max_duracion, num_intervals + 1)

            # Crear los intervalos de duración
            df_acum['duracion_intervalo'] = pd.cut(df_acum['duracion'], bins=limites_intervals, right=False)
            # Agrupa los datos por estos intervalos y suma solo la columna 'personas'
            df_agrupado = df_acum.groupby('duracion_intervalo').agg({
                'personas': 'sum',
            }).reset_index()

            df_agrupado['id_cc']=cc

            # Aplicar la función para crear la columna 'duracion'
            df_agrupado['duracion'] = df_agrupado['duracion_intervalo'].apply(calcular_punto_medio)
            horas_del_dia = pd.date_range(start='00:00:00', end='23:00:00', freq='H').time
            df_horas = pd.DataFrame({'hora': horas_del_dia})
            num_repeats = (len(df_agrupado) // len(day)) + 1
            df_agrupado['fecha'] = (day * num_repeats)[:len(df_agrupado)]
            df_agrupado['fecha'] = df_agrupado['fecha'].astype(str) + ' ' + df_horas['hora'].astype(str)
            df_agrupado['duracion_intervalo'] = df_agrupado['duracion_intervalo'].astype(str)
            # Insert the DataFrame into the table
            insert_df_into_table(df_agrupado,grouped, table_name,table_name1)

        else:
            break
            

start_time = time.time()
table='ingreso_persona_local2'
cc=[3]#   3 08-20      2,3        4,5,9,14,19
tinicial='09:00:00'
tfinal='20:00:00'
# Convertir las cadenas de tiempo a objetos datetime
for i in cc:
    fechas = getfechas(table, i)
    lista_fechas = []
    for fecha in fechas['fecha']:
        lista_fechas.append(fecha)
    fechas1= [fecha.replace('/', '-') for fecha in lista_fechas]
    principal(table,i,lista_fechas,fechas1,tinicial,tfinal)

end_time = time.time()
execution_time = end_time - start_time

# Imprimir el tiempo de ejecución
print(f"Tiempo de ejecución: {execution_time} segundos") 