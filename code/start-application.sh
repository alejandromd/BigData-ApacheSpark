#!/bin/bash

if [ $# -ne 2 ]; then
    echo "Uso: $0 <num_workers> <file_py>"
    exit 1
fi

num_workers="$1"
file_py="$2"

if [ "$num_workers" -lt 1 ]; then
    echo "El número de workers debe ser mayor o igual a 1"
    exit 1
fi

start_container() {
    local container_name=$1
    local container_command=$2
    if docker ps -a --format '{{.Names}}' | grep -Eq "^$container_name\$"; then
        if docker ps --format '{{.Names}}' | grep -Eq "^$container_name\$"; then
            echo "El contenedor $container_name ya está corriendo"
        else
            echo "Lanzando el contenedor $container_name..."
            docker start "$container_name"
            docker exec -it "$container_name" /bin/bash -c "$container_command"
            echo " "
        fi
    else
        echo "Creando el contenedor $container_name..."
        docker run -d --name "$container_name" spark-image /bin/bash -c "tail -f /dev/null"
        docker exec -it "$container_name" /bin/bash -c "$container_command"
        # Modify the log4j2.properties file to set the log level to error
        docker exec -it "$container_name" /bin/bash -c "cd /opt/spark-3.5.1-bin-hadoop3/conf && sed -i 's/rootLogger.level = info/rootLogger.level = error/' log4j2.properties.template && mv log4j2.properties.template log4j2.properties"
        echo " "
    fi
}

# Build the Docker image
if ! docker image inspect spark-image > /dev/null 2>&1; then
    echo "Construyendo la imagen Docker..."
    docker build -t spark-image .
fi

# Start the master container
start_container "spark-master" "cd /opt/spark-3.5.1-bin-hadoop3/sbin && ./start-master.sh -h 0.0.0.0"

# Get the IP of the master container
master_ip=$(docker inspect --format '{{ .NetworkSettings.IPAddress }}' spark-master)

# Start the worker containers
for ((i=1; i<=$num_workers; i++)); do
    start_container "spark-worker$i" "cd /opt/spark-3.5.1-bin-hadoop3/sbin && ./start-worker.sh spark://$master_ip:7077"
done

# Submit the Spark job
echo " "
echo "Para entrar a la interfaz de Spark accede en el navegador a: $master_ip:8080"
echo "Para ver los detalles de la ejecución accede en el navegador a: $master_ip:4040 o pulsa sobre el nombre de la aplicación al entrar a la interfaz de Spark"
echo " "
echo "Los resultados de la ejecución se mostrarán en la consola: "
echo " "
docker exec -it spark-master /bin/bash -c "cd /opt/spark-3.5.1-bin-hadoop3/bin && ./spark-submit --master spark://$master_ip:7077 --conf spark.driver.host=$master_ip --py-files /app/utils.py /app/$file_py"