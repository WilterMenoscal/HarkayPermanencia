import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from psycopg2 import sql
import psycopg2
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.express as px

# Configuración inicial

def conexionBase():
    conexion1 = psycopg2.connect(  host='localhost', port='5432', database='mylocal', user='mylocal', password='2001')
    return conexion1

#OBTENER DF
def getdf(table,cc):
    conexion1 = conexionBase()
    sql="select * from %s where id_cc=%s order by fecha" % (table,cc)
    df = pd.read_sql_query(sql, conexion1)
    conexion1.close()
    return df

def insert_df(df,table_name):
    try:
        # Establish connection to the database
        conexion1 = conexionBase()
        # Create a cursor object using the connection
        cursor = conexion1.cursor()     
        for index, row in df.iterrows():
            fecha = row['minuto']
            id_cc = row['id_cc']
            ins = row['ingresos']
            outs = row['salidas']
            # Convertir a float y manejar NaN o None
            if pd.isna(ins) or ins is None:
                ins = None  # o manejar según sea necesario
            
            if pd.isna(outs) or outs is None:
                outs = None  # o manejar según sea necesario
            
            # Define the insertion SQL statement
            insert_statement = sql.SQL("INSERT INTO {} (fecha,id_cc,ins,outs) VALUES (%s, %s, %s, %s)").format(
                sql.Identifier(table_name))

            # Execute the SQL statement with parameters
            cursor.execute(insert_statement, (fecha, id_cc,ins, outs))

        # Commit the transaction
        conexion1.commit()
        print("Data inserted successfully into table:", table_name)
    except psycopg2.Error as e:
        print("Error inserting data into PostgreSQL table:", e)
    
    finally:
        # Close cursor and connection
        if cursor:
            cursor.close()
        if conexion1:
            conexion1.close()


#Generar salidas metodo 1
def generar_salidas_df(df,intervalo_mananero,intervalo_tarde):
    # Convertir la columna 'fecha' a datetime
    df['fecha'] = pd.to_datetime(df['fecha'])
    
    # Asegurarse de que el DataFrame esté ordenado por fecha
    df = df.sort_values(by='fecha').reset_index(drop=True)

    # Dividir el DataFrame en dos partes: una para 08:00 a 14:00 y otra para el resto del día
    df_mananero = df[(df['fecha'].dt.time >= pd.to_datetime('08:00:00').time()) & 
                     (df['fecha'].dt.time < pd.to_datetime('14:00:00').time())]
    print(len(df_mananero))
    # Dividir el DataFrame en dos partes: una para 08:00 a 14:00 y otra para el resto del día
    df_tarde = df[(df['fecha'].dt.time >= pd.to_datetime('14:00:00').time()) & 
                     (df['fecha'].dt.time < pd.to_datetime('21:00:00').time())]
    print(len(df_tarde))
    # Mantener la proporción de 0.5 en cada parte(intervalos del dia)
    def aplicar_intervalo1(df_parte, intervalo_1):
        n_filas = len(df_parte)
        tiempos_salida = []
        tiempos_salida.extend(np.random.randint(intervalo_1[0], intervalo_1[1], size=n_filas))
        np.random.shuffle(tiempos_salida)
     
        return df_parte['fecha'] + pd.to_timedelta(tiempos_salida, unit='m')
    
    def aplicar_intervalo2(df_parte, intervalo_2):
        n_filas = len(df_parte)
        tiempos_salida = []
        tiempos_salida.extend(np.random.randint(intervalo_2[0], intervalo_2[1], size=n_filas))   
        np.random.shuffle(tiempos_salida)
        return df_parte['fecha'] + pd.to_timedelta(tiempos_salida, unit='m')

    # Aplicar los intervalos y mantener la proporción
    df_mananero['Salidas'] = aplicar_intervalo1 (df_mananero, intervalo_mananero)
    df_tarde['Salidas'] = aplicar_intervalo2(df_tarde, intervalo_tarde)
    
    # Unir ambas partes del DataFrame
    df_resultante = pd.concat([df_mananero, df_tarde]).sort_values(by='fecha').reset_index(drop=True)
    
    return df_resultante

#Generar salidas metodo 2
def generar_salidas_df2(df,proporciones,intervalos):
    # Convertir la columna 'fecha' a datetime
    df['fecha'] = pd.to_datetime(df['fecha'])
    
    # ordenar DF  por fecha
    df = df.sort_values(by='fecha').reset_index(drop=True)
    
    # Inicializar una lista para las fechas de salida
    fechas_salidas = []
    

    # Calcular la cantidad de salidas para cada intervalo
    total_filas = len(df)
    cantidades = [int(p * total_filas) for p in proporciones]
    
    # Ajuste si no se distribuyeron todas las filas debido a redondeo
    cantidades[-1] += total_filas - sum(cantidades)
    
    # Crear una lista de tiempos de salida respetando las proporciones
    tiempos_salida = []
    for cantidad, intervalo in zip(cantidades, intervalos):
        tiempos_salida.extend(np.random.randint(intervalo[0], intervalo[1], size=cantidad))
    
    # Barajar los tiempos de salida para que no sigan un patrón específico
    np.random.shuffle(tiempos_salida)
    
    # Generar las fechas de salida basadas en los tiempos
    for i in range(len(df)):
        fecha_ingreso = df.loc[i, 'fecha']
        fecha_salida = fecha_ingreso + pd.Timedelta(minutes=tiempos_salida[i])
        fechas_salidas.append(fecha_salida)
    
    # Agregar las fechas de salida al DataFrame original
    df['Salidas'] = fechas_salidas

    return df



def insouts(df):
    # Filtrar el DataFrame para eliminar registros sin fecha de salida
    df['fecha'] = pd.to_datetime(df['fecha'])
    df['Salidas'] = pd.to_datetime(df['Salidas'])

    # Crear nuevas columnas que contienen solo la fecha y hora redondeada al minuto
    df['minuto_ingreso'] = df['fecha'].dt.floor('T')
    df['minuto_salida'] = df['Salidas'].dt.floor('T')

    # Asegurarse de que las columnas son de tipo datetime
    df['minuto_ingreso'] = pd.to_datetime(df['minuto_ingreso'])
    df['minuto_salida'] = pd.to_datetime(df['minuto_salida'])

    # Contar los ingresos por minuto
    ingresos_por_minuto = df.groupby('minuto_ingreso').size().reset_index(name='ingresos')

    # Contar las salidas por minuto
    salidas_por_minuto = df.groupby('minuto_salida').size().reset_index(name='salidas')

    # Combinar los datos de ingresos y salidas en un solo DataFrame
    ingresos_salidas_por_minuto = pd.merge(ingresos_por_minuto, salidas_por_minuto, 
                                        left_on='minuto_ingreso', right_on='minuto_salida', 
                                        how='outer').fillna(0)

    # Asegurarse de que las columnas sean del tipo datetime antes de combinar
    ingresos_salidas_por_minuto['minuto_ingreso'] = pd.to_datetime(ingresos_salidas_por_minuto['minuto_ingreso'], errors='coerce')
    ingresos_salidas_por_minuto['minuto_salida'] = pd.to_datetime(ingresos_salidas_por_minuto['minuto_salida'], errors='coerce')

    # Combinar ambas columnas en una sola columna 'minuto'
    ingresos_salidas_por_minuto['minuto'] = ingresos_salidas_por_minuto[['minuto_ingreso', 'minuto_salida']].bfill(axis=1).iloc[:, 0]

    # Eliminar la columna duplicada 'minuto_salida'
    ingresos_salidas_por_minuto.drop(columns=['minuto_ingreso', 'minuto_salida'], inplace=True)

    # Convertir todos los valores de la columna 'minuto' a tipo datetime
    ingresos_salidas_por_minuto['minuto'] = pd.to_datetime(ingresos_salidas_por_minuto['minuto'], errors='coerce')

    # Ordenar el DataFrame por minuto
    ingresos_salidas_por_minuto.sort_values(by='minuto', inplace=True)
  
    return ingresos_salidas_por_minuto

def getdata(df,fecha):
    # Paso 1: Calcular la duración
    df['duracion'] = df['Salidas'] - df['fecha']
    # Paso 2: Convertir la duración a minutos
    df['duracion(m)'] = df['duracion'].dt.total_seconds() / 60
    df = df.drop('duracion', axis=1)
    # Aseguramos que la columna 'fecha' es de tipo datetime
        # Filtrar las filas que no tienen valores nulos en 'duracion_minutos'
    df_filtrado = df.dropna(subset=['duracion(m)'])
    print(f"Fecha: {fecha}, Registros: {len(df)}")
        # Agrupar por 'duracion_minutos' y contar el número de registros por duración
    tabla_duracion = df_filtrado.groupby('duracion(m)').size().reset_index(name='n_persona')
    # Ordenar la tabla por duración para una mejor visualización
    tabla_duracion = tabla_duracion.sort_values(by='duracion(m)').reset_index(drop=True)
    df['fecha'] = pd.to_datetime(df['fecha'])
    df['Salidas'] = pd.to_datetime(df['Salidas'])
    return df,tabla_duracion

def asignar_ids_unicos(grupo):
    # Asegúrate de que 'fecha' está en formato datetime
    grupo['fecha'] = pd.to_datetime(grupo['fecha'])
    
    # Crear una lista vacía para almacenar los nuevos registros
    registros = []
    count=1
    # Iterar sobre cada fila del DataFrame
    for index, row in grupo.iterrows():
        # Obtener la fecha y hora del ingreso
        fecha = row['fecha']
        ingresos = row['ins']
        
        # Generar IDs únicos para cada ingreso
        for i in range(int(ingresos)):
            # Crear un nuevo registro con el mismo tiempo de ingreso pero un ID único
            nuevo_registro = {
                'id': count,  # Crear un ID único basado en la fecha y un índice
                'fecha': fecha,
                'id_cc': row['id_cc'],
                'ins': 1,  # Cada fila representa un ingreso
            }
            registros.append(nuevo_registro)
            count=count+1
    # Crear un DataFrame a partir de los nuevos registros
    df_con_ids = pd.DataFrame(registros)
    
    return df_con_ids
def main(df2,dia,n,metodo,intervalo_mananero,intervalo_tarde,proporciones,intervalos,table_name):
    df2['id_cc']=1
    df2['fecha'] = pd.to_datetime(df2['fecha'])
    # Agrupar por cada día
    df2['fecha_dia'] = df2['fecha'].dt.date

    if dia=='unicos':
        # Obtener la primera fecha de la columna 'fecha_dia'
        primera_fecha = df2['fecha_dia'].min()
        print(primera_fecha)

        # Filtrar el DataFrame para obtener la siguiente fecha
        fechas_unicas = df2['fecha_dia'].unique()

        # Obtener el índice de la primera fecha
        indice_primera_fecha = list(fechas_unicas).index(primera_fecha)

        # Obtener la fecha siguiente
        if indice_primera_fecha + n < len(fechas_unicas):
            siguiente_fecha = fechas_unicas[indice_primera_fecha + n]
        else:
            siguiente_fecha = None  # No hay una fecha siguiente si la primera es la última

        print(siguiente_fecha)

        # Filtrar el DataFrame para incluir solo la primera fecha
        df_filtrado = df2[df2['fecha_dia'] == siguiente_fecha]

        grupos_por_dia = df_filtrado.groupby('fecha_dia')

    elif dia=='todos':
        df2['fecha_dia'] = df2['fecha'].dt.date
        grupos_por_dia = df2.groupby('fecha_dia')

    datos = []

    for fecha, grupo in grupos_por_dia:
        print(f"Procesando el día: {fecha}")
        df = grupo.drop(columns=['fecha_dia','outs'], errors='ignore')
        df['fecha'] = pd.to_datetime(df['fecha'])
        # Filtrar las filas donde la hora esté entre las 08:00 AM y las 21:00 PM
        inicio_filtro = '08:00:00'
        fin_filtro = '21:00:00'
        # Usar la función between_time para realizar el filtro
        df_filtrado = df[df['fecha'].dt.time.between(pd.to_datetime(inicio_filtro).time(), pd.to_datetime(fin_filtro).time())]
        grupo=asignar_ids_unicos(df_filtrado)

        if metodo=='metodo1':
            df = generar_salidas_df(grupo,intervalo_mananero,intervalo_tarde)
        elif metodo=='metodo2':
            df=  generar_salidas_df2(grupo,proporciones,intervalos)
        
        print(df)        
        #Eliminar ingresos
        #------------------------
        df1,tabla=getdata(df,fecha) #
        df2=insouts(df1)
        df2['id_cc']=1

        #Graficar duraciones del GT
        #graficarGT(tabla)
        #Si quiere guardar el resultado en otra fecha, usar, caso contario comentar
        df2['minuto'] = pd.to_datetime('2024-01-06') + pd.to_timedelta(df2['minuto'].dt.time.astype(str))
        print(df2)
        insert_df(df2,table_name)

#metodo1, generar salidas por intervalos de hora y proporciones
intervalo_mananero = (0, 30)
intervalo_tarde = (90, 120)

#metodo2, generar salidas del dia total, con diferentes intervalos 
proporciones = [0.5,0.5]  # 50%, 50%
intervalos = [(0,30),(90,120)]


tabla_original='inosuts2'
df2=getdf(tabla_original,1)
#dataframe y seleccionar metodo de generacion de salida
funsalidas='metodo1' # o metodo2
dia='unicos' # o todos, dias a procesar 
n=0 #Numero de dia a procesar, se usa si son dias unicos
table_name='insouts6' #en que tabla de insouts guardamos

main(df2,dia,n,funsalidas,intervalo_mananero,intervalo_tarde,proporciones,intervalos,table_name)
