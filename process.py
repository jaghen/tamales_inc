import pandas as pd
import glob
from pandas.api.types import is_numeric_dtype
from pandas.api.types import is_string_dtype
from pandas.api.types import is_datetime64_any_dtype as is_datetime
import os
from datetime import datetime

#Funciones

#Leer archivos y cargarlos en un dataframe
def read_files(path,header):
    df_union=[]
    files = glob.glob(path)
    for f in files:
        df = pd.read_csv(f,names=header,sep=',')
        df['source'] = f
        df_union.append(df)
    df_master = pd.concat(df_union,sort=False,ignore_index=True) 
    return df_master

#Limpieza y estandarizacion de datos
def clean_data(df):
    num_cols = [cols for cols in df.columns if is_numeric_dtype(df[cols]) and len(df[cols].dropna())>0]
    iter_len_num = len(num_cols)
    
    string_cols = [cols for cols in df.columns if is_string_dtype(df[cols]) and len(df[cols].dropna())>0]
    iter_len_string = len(string_cols)
    
    
    #Eliminar filas con todos los datos vacios
    df.dropna(how = 'all')
    
    #para campos numericos
    print('Limpieza de campos numericos:')
    for x,col_name in enumerate(num_cols):       
        #En campos numericos, reemplazar valores nulos por 0 
        df[col_name] = df[col_name].fillna(0)
        df[col_name] = pd.to_numeric(df[col_name])
        df[col_name] = df[col_name].astype(int)
        print(x+1,' of ',iter_len_num,' completado ',col_name)
        
    #para campos de tipo string    
    print('Limpieza de campos de tipo cadena:')
    for x,col_name in enumerate(string_cols):        
        #Eliminar espacios en blaco para cadenas
        df[col_name] = df[col_name].str.strip()
        #Reemplazar cadenas vacias
        df[col_name] = df[col_name].fillna('N/D')
        print(x+1,' of ',iter_len_string,' completado ',col_name)
    #Quitamos ventas con valores negativos o cero
    return df


###-Declarar variables - Rutas de archivos y encabezados-###

#Ventas de tamal
path_files_ventas = 'tamales_inc/ventas_mensuales_tamales_inc/mx/*/*/*/*'
ventas_mensuales_head = ['year','month','country','calorie_category','flavor','zone','product_code','product_name','sales']

#fact_table
path_files_mercado_fact_table = 'teinvento_inc/ventas_reportadas_mercado_tamales/mx/*/fact_table/*.csv'
ventas_mercado_fact_table_head = ['year','month','sales','id_region','id_product']

#product_dim
path_files_mercado_product_dim = 'teinvento_inc/ventas_reportadas_mercado_tamales/mx/*/product_dim/*.csv'
ventas_mercado_product_dim_head = ['id_product','calorie_category','product','product_brand','producer']

#region_dim
path_files_mercado_region_dim = 'teinvento_inc/ventas_reportadas_mercado_tamales/mx/*/region_dim/*.csv'
ventas_mercado_region_dim_head = ['id_region','country','region']


###-Cargar datos-###

#ventas de tamales
df_ventas_tamales_crudo = read_files(path_files_ventas,ventas_mensuales_head)
#fact_table
df_fact_table_crudo = read_files(path_files_mercado_fact_table,ventas_mercado_fact_table_head)
#product_dim
df_product_dim_crudo = read_files(path_files_mercado_product_dim,ventas_mercado_product_dim_head)
#region_dim
df_region_dim_crudo = read_files(path_files_mercado_region_dim,ventas_mercado_region_dim_head)

print('Proceso de carga terminado.')

#limpieza de datos

#Identificar valores nulos
df_ventas_tamales_crudo.isnull().sum()
df_fact_table_crudo.isnull().sum()
df_product_dim_crudo.isnull().sum()
df_region_dim_crudo.isnull().sum()

#Ejecutar funcion de limpieza de datos

df_ventas_tamales_crudo = clean_data(df_ventas_tamales_crudo)
df_fact_table_crudo = clean_data(df_fact_table_crudo)
df_product_dim_crudo = clean_data(df_product_dim_crudo)
df_region_dim_crudo = clean_data(df_region_dim_crudo)
print('Proceso de limpieza terminado.')

#Quitar ventas con valores negativos y cero

df_ventas_tamales_crudo=df_ventas_tamales_crudo[df_ventas_tamales_crudo['sales']>0]
df_fact_table_crudo=df_fact_table_crudo[df_fact_table_crudo['sales']>0]
#Procesamiento de datos, generar cubo de informacion.

#---Métricas a integrar:
#1) Ventas mensuales
#2) Ventas mensuales acumuladas
#3) Diferencia % vs el mes anterior

#---Agregaciones importantes para el cubo:
#1) Producto, marca
#2) Año, mes
#3) Estado

print('Generando cubo...')

df_procesado = df_fact_table_crudo.merge(df_product_dim_crudo, 
                          left_on='id_product', 
                          right_on='id_product', 
                          how='left', 
                          suffixes=('_fact', '_product')) 

df_procesado = df_procesado.merge(df_region_dim_crudo, 
                          left_on='id_region', 
                          right_on='id_region', 
                          how='left', 
                          suffixes=('_procesado', '_region'))

##Teinvento Inc.
#Ventas mensuales
df_procesado['month_period'] = pd.to_datetime(df_procesado.month, format='%b', errors='coerce').dt.month
df_procesado_result = df_procesado.groupby(['year','month','month_period','region','product','producer'],as_index=False).agg({'sales':'sum'})
df_procesado_result = df_procesado_result.sort_values(by = ['year','month_period','region','producer','product'],ascending=[True,True,True,True,True])
#Ventas mensuales acumuladas
df_procesado_result['sales_cumulative'] = df_procesado_result.groupby(['region','product'])['sales'].apply(lambda x: x.cumsum())
#Dif entre periodos
df_procesado_result = df_procesado_result.sort_values(by = ['year','month_period','region','product'],ascending=[False,False,False,False])
df_procesado_result['dif_vs_prev'] = df_procesado_result.groupby(['region','product'])['sales'].diff(periods=-1)
#Resultado
df_procesado_result = df_procesado_result.sort_values(by = ['region','product','year','month_period'],ascending=[False,False,False,False])
df_procesado_result = df_procesado_result[['year','month','region','product','sales','sales_cumulative','dif_vs_prev']]


#ERP Tamales Inc.
#Ventas mensuales
df_ventas_tamales_crudo['month_period'] = pd.to_datetime(df_ventas_tamales_crudo.month, format='%b', errors='coerce').dt.month
df_procesado_result_tamales_erp = df_ventas_tamales_crudo.groupby(['year','month','month_period','zone','product_name'],as_index=False).agg({'sales':'sum'})
df_procesado_result_tamales_erp = df_procesado_result_tamales_erp.sort_values(by = ['year','month_period','zone','product_name'],ascending=[True,True,True,True])
#Ventas mensuales acumuladas
df_procesado_result_tamales_erp['sales_cumulative'] = df_procesado_result_tamales_erp.groupby(['zone','product_name'])['sales'].apply(lambda x: x.cumsum())
#Dif entre periodos
df_procesado_result_tamales_erp = df_procesado_result_tamales_erp.sort_values(by = ['year','month_period','zone','product_name'],ascending=[False,False,False,False])
df_procesado_result_tamales_erp['dif_vs_prev'] = df_procesado_result_tamales_erp.groupby(['zone','product_name'])['sales'].diff(periods=-1)
df_procesado_result_tamales_erp = df_procesado_result_tamales_erp.sort_values(by = ['zone','product_name','year','month_period'],ascending=[False,False,False,False])
#Resultado
df_procesado_result_tamales_erp = df_procesado_result_tamales_erp[['year','month','zone','product_name','sales','sales_cumulative','dif_vs_prev']]


print('Cubos terminados...')

# Salidas de datos crudos a csv
print('Generando salidas...')

#Salidas de datos procesados a csv

#crear directorios de salida

today = datetime.today().strftime('%Y%m%d')

crudo_output_path = 'resultados/crudo/generador/fuente/'+today+'/'
procesado_output_path = 'resultados/procesado/generador/fuente/'+today+'/'

if not os.path.exists(crudo_output_path):
    os.makedirs(crudo_output_path)

if not os.path.exists(procesado_output_path):
    os.makedirs(procesado_output_path)

#salida a csv

#crudo
df_ventas_tamales_crudo = df_ventas_tamales_crudo.iloc[:,0:10]

df_ventas_tamales_crudo.to_csv(crudo_output_path+'ventas_tamales_crudo_'+today+'.csv', index=False)
df_fact_table_crudo.to_csv(crudo_output_path+'fact_table_crudo_'+today+'.csv', index=False)
df_product_dim_crudo.to_csv(crudo_output_path+'product_dim_crudo_'+today+'.csv', index=False)
df_region_dim_crudo.to_csv(crudo_output_path+'region_dim_crudo_'+today+'.csv', index=False)

#df_ventas_tamales_crudo.sales = df_ventas_tamales_crudo.sales.astype(int)
#procesado

df_procesado_result.to_csv(procesado_output_path+'cubo_teinvento_'+today+'.csv', index=False)
df_procesado_result_tamales_erp.to_csv(procesado_output_path+'cubo_ventas_tamales_erp_'+today+'.csv', index=False)

print('Fin del proceso.')