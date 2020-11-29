import pandas as pd
import glob
from pandas.api.types import is_numeric_dtype
from pandas.api.types import is_string_dtype
from pandas.api.types import is_datetime64_any_dtype as is_datetime

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
        print(x+1,' of ',iter_len_num,' completado ',col_name)
    #para campos de tipo string    
    print('Limpieza de campos de tipo cadena:')
    for x,col_name in enumerate(string_cols):        
        #Eliminar espacios en blaco para cadenas
        df[col_name] = df[col_name].str.strip()
        #Reemplazar cadenas vacias
        df[col_name] = df[col_name].fillna('N/D')
        print(x+1,' of ',iter_len_string,' completado ',col_name)
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



