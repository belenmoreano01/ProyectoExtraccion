# Proyecto Implementación de Bodega de Datos
Nombre: Andrea Moreano 

Carrera: Ingeniería de Software

# Descripción del Proyecto
En Staging crear las tablas independientes respectivas y extraer los datos de todos los 7 archivos CSV adjuntados para depositarlos en las respectivas tablas de Staging

# Como ejecutar el proyecto
1.Es necesario clonar el repositorio

2.Abrir la carpeta en Visual Studio Code

3.Crear las dos tablas en MySQL (staging y sore), con los scripts subidos al repositorio en la carpeta "Base de datos MySQL"

4.Instalar las siguientes librerías:

    - configparser
    
    - pymysql
    
    - pandas
    
    Para instalarlas se puede hacer directamente desde el terminal con el comando "pip install <nombreLibrería>
    
5.En el archivo "data.properties", cambiar a los parámetros correspondientes     

4.Abrir el terminal y colocar el comando "python  .\py_startup.py". Es importante mencionar que en el archivo 'py_starup.py', están comentadas las tablas, y si se requiere probar alguna de forma individual, es necesario descomentar la requerida    
