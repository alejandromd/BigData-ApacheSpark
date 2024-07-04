# Proyecto de Big Data con Apache Spark
Para una guía de instalación con más detalle se recomienda consultar la memoria del proyecto, esto se trata de un resumen de la misma.

## Manual de uso
### Jupyter Notebook
El proyecto se divide en dos partes, por lo que para la ejecución y lectura del cuaderno Jupyter es necesario realizar la descarga de **Jupyter Notebook** y la **instalación de PySpark**.

Al acabar este proceso, para la ejecución de Graphframes será necesario la descarga del [jar](https://spark-packages.org/package/graphframes/graphframes) correspondiente a la versión de Scala que se tenga instalada, y ponerla dentro del directorio jars de spark, por ejemplo: `“C:\spark-3.5.1-bin-hadoop3\jars”`

### Cluster

Es necesaria la instalación de `Docker` para el despliegue en local. La instalación del entorno es muy sencilla, hemos creado tres archivos ".sh” que agilizan todo el proceso. Una vez se ha descargado y descomprimido el archivo “ApacheSpark-BigData.zip”, debemos de dirigirnos a la carpeta “code” en la consola, una vez ahí simplemente debemos de ejecutar el comando:

`./start-application.sh 4 RandomForest.py`

> Nota: Este comando construye la imagen Docker en caso de que no lo esté previamente

Los parámetros del comando son, en primer lugar el número de nodos trabajadores que quiere lanzar para ejecutar la aplicación, y el segundo es el nombre del archivo a ejecutar (estos dos parámetros son configurables, se pueden cambiar según se considere, no recomendando poner una cantidad elevada por el consumo de recursos). Una vez ejecutado este comando se crearán e iniciarán los contenedores, uno para el nodo maestro y los seleccionados para los trabajadores, en caso de que no lo estén, formando un clúster automáticamente y ejecutando la aplicación elegida.

Si se devuelve “Permiso denegado” se debe de ejecutar antes este comando para cada script “.sh”:

`chmod +x start-application.sh`

`chmod +x stop-application.sh`

`chmod +x start-application-streaming.sh`

Los archivos que podremos ejecutar de esta forma son todos los “.py” que estén dentro de la carpeta, menos “utils.py” el cual es usado para almacenar las funciones y “streaming.py” que al ser una extensión de la API de Spark se lanza de forma distinta.

#### Spark Streaming
El despliegue de “streaming.py”, se hace de manera similar a la anterior pero con:

`./start-application-streaming.sh`

Posteriormente, puede que aparezca algún error de “Conexión rechazada”, esto es por que aún no hay ningún servidor escuchando, deja esa terminal abierta y abra una nueva, en ella ponga:
`docker exec -it spark-master /bin/bash`
Y una vez dentro del contenedor:  `nc -lk 9999`

Ahora podrá escribir consultas SQL en esta nueva terminal, mientras que la primera consola recibe las peticiones y devuelve la salida correspondiente.

#### Parar y borrar aplicación

Para detener los contenedores, y borrarlos en caso de que se quiera, está el siguiente script que se ha creado para agilizar el proceso:

`./stop-application.sh`

Al ejecutar este comando todos los contenedores se detendrán y saldrán dos preguntas por consola, las cuales son para elegir si borrar los contenedores creados para la aplicación y para eliminar la imagen de Docker. Tras esto, todos los recursos usados para la aplicación habrán sido liberados.

