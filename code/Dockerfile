# Usar una imagen base de Ubuntu con Python 3.8
FROM ubuntu:20.04

# Evitar que se soliciten interacciones de usuario durante la construcción de la imagen
ENV DEBIAN_FRONTEND=noninteractive

# Instalar Python, Java 8 y otros paquetes necesarios
RUN apt-get update && apt-get install -y \
    openjdk-8-jdk \
    python3.8 \
    python3-pip \
    nano \
    wget \
    netcat

# Establecer variables de entorno para Java y Spark
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV SPARK_HOME=/opt/spark-3.5.1-bin-hadoop3
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9-src.zip:$PYTHONPATH
ENV PYSPARK_PYTHON=python3.8
ENV PYSPARK_DRIVER_PYTHON=python3.8

# Descargar e instalar Spark
RUN wget https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz \
    && tar -xvf spark-3.5.1-bin-hadoop3.tgz \
    && mv spark-3.5.1-bin-hadoop3 /opt \
    && rm spark-3.5.1-bin-hadoop3.tgz

# Descargar el archivo graphframes-0.8.3-spark3.5-s_2.12.jar
RUN wget https://repos.spark-packages.org/graphframes/graphframes/0.8.3-spark3.5-s_2.12/graphframes-0.8.3-spark3.5-s_2.12.jar -P /opt/spark-3.5.1-bin-hadoop3/jars/

# Instalar las librerías de Python necesarias
RUN pip3 install pyspark==3.5.1 numpy pandas findspark graphframes

# Copiar los archivos corrected.gz, kddcup.data.gz y kddcup.data_10_percent.gz al directorio bin de Spark
COPY corrected.gz kddcup.data.gz kddcup.data_10_percent.gz /opt/spark-3.5.1-bin-hadoop3/bin/

# Copiar el código fuente al contenedor
COPY . /app
WORKDIR /app