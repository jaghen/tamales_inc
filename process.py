import pandas as pd
import glob

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

#limpieza de datos

