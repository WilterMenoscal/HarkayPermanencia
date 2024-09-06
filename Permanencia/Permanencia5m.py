import numpy as np
import pandas as pd
from datetime import datetime, timedelta, date
from psycopg2 import sql
import psycopg2
from plotly.subplots import make_subplots
import plotly.express as px


# Configuración inicial

def conexionBase():
    conexion1 = psycopg2.connect(  host='localhost', port='5432', database='mylocal', user='mylocal', password='2001')
    return conexion1

#OBTENER DF
def getdf(table,cc):
    conexion1 = conexionBase()
    sql= "SELECT *FROM %s WHERE id_cc=%s and EXTRACT(YEAR FROM TO_TIMESTAMP(fecha, 'YYYY-MM-DD HH24:MI:SS')) = 2024 order by fecha"  % (table,cc)
    df = pd.read_sql_query(sql, conexion1)
    conexion1.close()
    return df

#Insertar datos a la DB(insouts) 
def insert_df(df,table_name):
    try:
        #Coneccion a la base de datos
        conexion1 = conexionBase()
        # Create a cursor object using the connection
        cursor = conexion1.cursor()     
        for index, row in df.iterrows():
            fecha = row['fecha']
            id_cc = row['id_cc']
            ins = row['ins']
            outs = row['outs']
            # Manejar NaN o None
            if pd.isna(ins) or ins is None:
                ins = None 
            if pd.isna(outs) or outs is None:
                outs = None 
            
            #Condicion Insertar SQL
            insert_statement = sql.SQL("INSERT INTO {} (fecha,id_cc,ins,outs) VALUES (%s, %s, %s, %s)").format(
                sql.Identifier(table_name))

            #Ejecutar SQL y insertar parametros
            cursor.execute(insert_statement, (fecha, id_cc,ins, outs))

        # Commit SQL
        conexion1.commit()
        print("Data inserted successfully into table:", table_name)
    except psycopg2.Error as e:
        print("Error inserting data into PostgreSQL table:", e)
    
        # cerrar cursor y conexion
    finally:
        if cursor:
            cursor.close()
        if conexion1:
            conexion1.close()

#Insertar datos a la DB(freq_datos) 
def insert_df2(df,table_name):
    try:
        #Coneccion a la base de datos
        conexion1 = conexionBase()
        # Create a cursor object using the connection
        cursor = conexion1.cursor()     
        for index, row in df.iterrows():
            fecha = row['hora']
            id_cc = row['id_cc']
            cumintout = row['cumincumout']
            duracion = row['duracionH_avg']
            salidas = row['personas']
            # Manejar NaN o None
            if pd.isna(cumintout) or cumintout is None:
                cumintout = None  # o manejar según sea necesario
            
            if pd.isna(duracion) or duracion is None:
                duracion = None  # o manejar según sea necesario

            if pd.isna(salidas) or salidas is None:
                salidas = None  # o manejar según sea necesario

            #Condicion Insertar SQL
            insert_statement = sql.SQL("INSERT INTO {} (fecha,id_cc,cumincumout,duracionh,salidas) VALUES (%s, %s, %s,%s, %s)").format(
                sql.Identifier(table_name))

            #Ejecutar SQL y insertar parametros
            cursor.execute(insert_statement, (fecha, id_cc,cumintout, duracion,salidas))

        # Commit SQL
        conexion1.commit()
        print("Data inserted successfully into table:", table_name)
    except psycopg2.Error as e:
        print("Error inserting data into PostgreSQL table:", e)
    
    finally:
        # cerrar cursor y conexion
        if cursor:
            cursor.close()
        if conexion1:
            conexion1.close()

#Agrupar datos cada 5 minutos
def agrupar5m(grupo,n_days):
    # Convertir la columna 'fecha' a formato datetime
    grupo['fecha'] = pd.to_datetime(grupo['fecha'])

    # Sumar n días a la columna 'fecha' para desplazamiento si es necesario
    grupo['fecha'] = grupo['fecha'] + pd.Timedelta(days=n_days) 
    
    #Convertir la columna 'fecha' a tipo datetime
    grupo['fecha'] = pd.to_datetime(grupo['fecha'])

    # Establecer 'fecha' como índice
    grupo.set_index('fecha', inplace=True)

    # Agrupar los datos cada 5 minutos y sumar
    grupo = grupo.resample('5T').sum()

    # Resetear el índice para obtener 'fecha' como columna
    grupo.reset_index(inplace=True)
    grupo['id_cc']=cc
    print(grupo)
    
    return grupo

#Obtener acumulados y generar datos
def correccion(df):
    # Convertir la columna 'fecha' a un objeto datetime
    df['fecha'] = pd.to_datetime(df['fecha'])
    
    # Calcular los acumulados de 'ins' y 'outs' por fecha
    df['ins_acumulados'] = df.groupby(df['fecha'].dt.date)['ins'].cumsum()
    df['outs_acumulados'] = df.groupby(df['fecha'].dt.date)['outs'].cumsum()
    df['cumin-cumout'] = df['ins_acumulados'] - df['outs_acumulados']
    # Obtener el valor más grande de 'ins_acumulados' y 'outs_acumulados'
    max_ins = df['ins_acumulados'].max()
    max_outs = df['outs_acumulados'].max()
    
    # Obtener escala, si exiten mas salidas que ingresos se calcula, sino se establece en 1
    if max_outs>max_ins:
        #Dividir el valor más grande de 'outs_acumulados' por el de 'ins_acumulados'
        escala = max_outs / max_ins if max_ins != 0 else None
    else:
        escala=1

    df['ins'] = df['ins'] * escala
    df['ins'] = np.ceil(df['ins'])
    print(df)

    return df

#Obtener tabla de ingresos y salidas
def insouts(df):
    entradas={}
    salidas={}    
    #Recorrer los datos y crear ingresos y salidas
    for idx in df.index:
        date = df.loc[idx, 'fecha']
        ingresos = df.loc[idx, 'ins']
        salida = df.loc[idx, 'outs']
        if ingresos != 0:
            entradas[idx] = (date, ingresos)
        if salida != 0:
            salidas[idx] = (date, salida)

    #Obtener  valores totales de ingresos y salidas durante el dia        
    suma_total = sum(value[1] for key, value in entradas.items())
    print("Ingresos:", suma_total)
    suma_total = sum(value[1] for key, value in salidas.items())
    print("Salidas:", suma_total)

    # Convertir los diccionarios a DataFrames para facilitar la graficación
    df_entradas = pd.DataFrame.from_dict(entradas, orient='index', columns=['fecha', 'ins'])
    df_salidas = pd.DataFrame.from_dict(salidas, orient='index', columns=['fecha', 'outs'])

    # Ordenar los DataFrames por 'datetime'
    df_entradas = df_entradas.sort_values(by='fecha')
    df_salidas = df_salidas.sort_values(by='fecha')
    return df_entradas,df_salidas

#Calcular las duraciones, en donde se ve cada evento de salida y busca en eventos de entradas pasados hasta ese preciso momento, 
#Si existe el numero total de salidas en los ingresos en el pasado, se calcula la digerencia de tiempos
def duration(entradas_df, salidas_df):
    # Convertir las horas a objetos datetime
    entradas_df['hora'] = pd.to_datetime(entradas_df['fecha'], format='%H:%M:%S').dt.time
    salidas_df['hora'] = pd.to_datetime(salidas_df['fecha'], format='%H:%M:%S').dt.time
    entradas_df_copy = entradas_df.copy()

    resultado = []
    resultado3 = []
    personas_sin_entrada = 0  # Contador para las personas sin entrada correspondiente
    personas_sin_entrada_hora = []  # Lista para registrar las horas de salida sin entrada correspondiente

    # Iterar sobre las salidas
    for idx_salida, row_salida in salidas_df.iterrows():
        hora_salida = row_salida['hora']
        personas_salidas = row_salida['outs']
        personas_restantes_salir = personas_salidas

        # Filtrar entradas hasta la hora de salida
        entradas_filtradas = entradas_df_copy[entradas_df_copy['hora'] <= hora_salida]

        # Iterar sobre las entradas menores al tiempo de salida
        for idx_entrada, row_entrada in entradas_filtradas.iterrows():
            hora_entrada = row_entrada['hora']
            personas_entradas = row_entrada['ins']
            if personas_entradas > 0:
                if personas_restantes_salir <= personas_entradas:
                    # Restar las personas que salieron de las entradas disponibles
                    entradas_df_copy.at[idx_entrada, 'ins'] -= personas_restantes_salir
                    # Calcular la duración y guardar el resultado
                    duracion = (datetime.combine(datetime.today(), hora_salida) - datetime.combine(datetime.today(), hora_entrada)).total_seconds() / 60
                    resultado.append({'duracion': duracion, 'personas': personas_restantes_salir, 'fecha': row_salida['fecha']})
                    resultado3.append({'duracion': duracion, 'personas': personas_restantes_salir, 'hora_salida': hora_salida, 'fecha': row_salida['fecha']})
                    personas_restantes_salir = 0
                    break
                else:
                    # Caso especial: más salidas que entradas, procesar parcialmente
                    personas_restantes_salir -= personas_entradas
                    duracion = (datetime.combine(datetime.today(), hora_salida) - datetime.combine(datetime.today(), hora_entrada)).total_seconds() / 60
                    resultado.append({'duracion': duracion, 'personas': personas_entradas, 'fecha': row_salida['fecha']})
                    resultado3.append({'duracion': duracion, 'personas': personas_entradas, 'hora_salida': hora_salida, 'fecha': row_salida['fecha']})
                    entradas_df_copy.at[idx_entrada, 'ins'] = 0

        # Si quedan personas sin entrada correspondiente, sumarlas al contador y registrar la hora
        if personas_restantes_salir > 0:
            personas_sin_entrada += personas_restantes_salir
            personas_sin_entrada_hora.append({'hora_salida': hora_salida, 'personas_sin_entrada': personas_restantes_salir})

    # Convertir las listas de resultados a DataFrames
    resultado_df = pd.DataFrame(resultado)
    resultado3_df = pd.DataFrame(resultado3)
    
    # Extraer fechas únicas
    fechas_unicas = resultado_df['fecha'].dt.date.unique()

    # Convertir resultados a listas para mayor claridad
    lista_fechas_unicas = fechas_unicas.tolist()
    acumulados_df = resultado_df.groupby(['duracion'], as_index=False)['personas'].sum()

    # Añadir una columna 'fecha' en acumulados_df con valores de lista_fechas_unicas
    num_repeats = (len(acumulados_df) // len(lista_fechas_unicas)) + 1
    acumulados_df['fecha'] = (lista_fechas_unicas * num_repeats)[:len(acumulados_df)]

    print(f"Personas sin entrada correspondiente: {personas_sin_entrada}")
    print(acumulados_df)
    
    max_value = resultado3_df['duracion'].max()
    print('max value \n', max_value)

    # Mostrar las horas de salida sin entrada correspondiente
    if personas_sin_entrada_hora:
        print("Horas de salida sin entrada correspondiente:")
        for entry in personas_sin_entrada_hora:
            print(f"Hora de salida: {entry['hora_salida']}, Personas: {entry['personas_sin_entrada']}")

    return resultado3_df, acumulados_df

#Calcular la duracion final cuando se tiene diferentes duraciones en el mismo tiempo. (duracionxpersona / persona)
def permanenciafinal(df):
    # Calcular 'duracionxpersona'
    df['duracionxpersona'] = df['duracion'] * df['personas']
    
    # Agrupar por 'hora_salida' y sumar los valores de 'duracionxpersona' y 'personas'
    df_agrupado = df.groupby(['fecha', 'hora_salida'], as_index=False).agg({
        'duracionxpersona': 'sum',
        'personas': 'sum'
    })
    # Calcular 'duracionH'
    df_agrupado['duracionH'] = df_agrupado['duracionxpersona'] / df_agrupado['personas']
    # Mostrar resultados
    df_agrupado['hora_salida'] = df_agrupado['hora_salida'].astype(str)
    return df_agrupado

def graficoduracionpromedio(acumulados_df):

    # Encontrar la fila con el máximo número de personas
    max_personas_row = acumulados_df.loc[acumulados_df['personas'].idxmax()]
    max_duracion = max_personas_row['duracion']
    max_personas = max_personas_row['personas']

    # Crear el gráfico de línea
    fig = px.line(acumulados_df, x='duracion', y='personas', title='Duración vs Personas')
    fig.update_layout(
        xaxis_title='Duración (minutos)',
        yaxis_title='Número de Personas',
        title={
            'text': 'Relación entre Duración y Número de Personas',
            'x': 0.5,
            'xanchor': 'center'
        }
    )
    fig.update_xaxes(
        tickfont=dict(size=56)  # Cambia el tamaño según tus preferencias
    )
    fig.add_shape(
        type="line",
        x0=max_duracion, x1=max_duracion, y0=0, y1=max_personas,
        line=dict(color="Red", width=2, dash="dashdot"),
    )
    fig.show()

def df_procesado(grupo,df3):
    grupo['hora'] = grupo['fecha'].dt.strftime('%Y-%m-%d %H:00:00')
    grupo =grupo.groupby('hora').agg({
        'ins': 'sum',  # Sumar las duraciones
        'outs': 'sum',  # Sumar las personas
    }).reset_index()

    df3['ins_acumulados'] = grupo.groupby('hora')['ins'].cumsum()
    df3['outs_acumulados'] = grupo.groupby('hora')['outs'].cumsum()
    df3['cumincumout'] = df3['ins_acumulados'] - df3['outs_acumulados']
    df3.drop(columns=['ins_acumulados', 'outs_acumulados','duracionxpersona'], inplace=True)
    df3['hora'] = df3['fecha'].dt.floor('H')
    #GRAFICO POR MINUTOS
    # Agrupar por la nueva columna 'hora'
    df3['num']=1
    grouped = df3.groupby('hora').agg({
        'duracionH': 'sum',     # Sumar 'duracionH'
        'personas': 'sum',          # Sumar 'outs'
        'num': 'count'        # Contar los registros para promediar más tarde
    }).reset_index()
    # Calcular el promedio de cada columna sumada
    grouped['duracionH_avg'] = grouped['duracionH'] / grouped['num']
    grouped['id_cc'] = cc
    grouped['cumincumout']=0
    grouped.drop(columns=['num', 'duracionH'], inplace=True)
    # print(grouped)
    grouped['fecha'] = pd.to_datetime(grouped['hora'])
    grouped['fecha_dia'] = grouped['fecha'].dt.date
    primera_fecha = grouped['fecha_dia'].min()
    fechas_unicas = grouped['fecha_dia'].unique()
    date = pd.to_datetime(fechas_unicas)
    date_str = date.strftime('%Y-%m-%d')
    tfinal='23:00:00'
    # Concatenar la fecha y la hora con un espacio
    datetime_str = date_str[0] + ' ' + tfinal
    datetime_obj = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')

    # Ahora puedes usar strftime en el objeto datetime
    day3 = datetime_obj.strftime('%Y-%m-%d %H:%M:%S')

    nueva_fila = {
        'hora': day3,  # Puedes especificar el valor que desees
        'personas': 0,
        'duracionH_avg':0,
        'id_cc': cc
    }
    # Convertir el diccionario a un DataFrame de una sola fila
    nueva_fila_df = pd.DataFrame([nueva_fila])
    # Agregar la nueva fila al DataFrame existente
    groupedfinal = grouped._append(nueva_fila_df, ignore_index=True)
    groupedfinal['cumincumout']=0
    print(groupedfinal)
    return groupedfinal

cc_id=[3]
table_read='insouts7'
table_write_IO='insouts8'
table_write_perm='freq_datos4'
n_days=0
for i in cc_id:
    cc=i
    df2=getdf(table_read,cc)
    df2['id_cc']=cc
    df2['fecha'] = pd.to_datetime(df2['fecha'])
    df2['fecha_dia'] = df2['fecha'].dt.date
    grupos_por_dia = df2.groupby('fecha_dia')

    # a En caso de querer procesar solamente el primer dia para testear, usar las siguientes lineas
    # primera_fecha = df2['fecha_dia'].min()
    # df_filtrado = df2[df2['fecha_dia'] == primera_fecha]
    #grupos_por_dia = df_filtrado.groupby('fecha_dia')

    # Iterar sobre cada día
    for fecha, grupo in grupos_por_dia:
        
        #Eliminar columnas innecesarias
        grupo = grupo.drop(columns=['hour','fecha_dia'], errors='ignore')
        
        #Agrupar por 5 min y desplazar, si no desea omitir esta linea
        grupo=agrupar5m(grupo,n_days)
        
        #Corregir ingresos y guardarlos
        grupo=correccion(grupo)
        insert_df(grupo,table_write_IO)
        
        #Separar en ins y outs
        entradas,salidas=insouts(grupo)

        #Calcular duracion y permanencia
        resultado,acumulados_df=duration(entradas,salidas)
        df3=permanenciafinal(resultado)

        #Proceso final de datos y guardado
        groupedfinal=df_procesado(grupo,df3)
        insert_df2(groupedfinal,table_write_perm)
        #graficoduracionpromedio(acumulados_df)