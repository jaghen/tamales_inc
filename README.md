# tamales_inc
Proceso de carga de datos y procesamiento para generar cubo con metricas y kpi's

Caso practico

Instrucciones de ejecución.

1. Para ejecutar el proceso, es necesario tener instalado Python 3.6 como mínimo.

2. Una vez instalado Python, se debe de crear un ambiente virtual:

python -m venv tamales

3. Activamos el ambiente virtual con el siguiente codigo:

source ./tamales/bin/activate

4. Instalar los requerimientos necesarios para correr nuestro proyecto:

pip install -r requirements.txt

5. Ejecutar el código con la instrucción:

python process.py

El proceso se ejecutara paso por paso, hasta que nos indique que ha concluido.

Limpieza de campos de tipo cadena:
1  of  3  completado  country
2  of  3  completado  region
3  of  3  completado  source
Proceso de limpieza terminado.
Generando cubo...
Cubos terminados...
Generando salidas...
Fin del proceso.




