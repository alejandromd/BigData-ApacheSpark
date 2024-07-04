#!/bin/bash

stop_container() {
    local container_name=$1
    if docker ps --format '{{.Names}}' | grep -Eq "^$container_name\$"; then
        echo "Deteniendo el contenedor $container_name..."
        docker stop "$container_name"
    else
        echo "El contenedor $container_name no está corriendo"
    fi
}

start_container() {
    local container_name=$1
    local container_command=$2
    if docker ps -a --format '{{.Names}}' | grep -Eq "^$container_name\$"; then

        # Stop the master container
        stop_container "spark-master"
        
        echo "Lanzando el contenedor $container_name..."
        docker start "$container_name"
        container_id=$(docker ps -aqf "name=spark-master")
        docker exec -it "$container_name" /bin/bash -c "$container_command && ./start-worker.sh spark://$container_id:7077"
        docker exec -it "$container_name" /bin/bash -c "cd /opt/spark-3.5.1-bin-hadoop3/bin && ./spark-submit --master spark://$container_id:7077 /app/streaming.py"
        
    else
        echo "Creando el contenedor $container_name..."
        docker run -d --name "$container_name" spark-image /bin/bash -c "tail -f /dev/null"
        # Modify the log4j2.properties file to set the log level to error
        docker exec -it "$container_name" /bin/bash -c "cd /opt/spark-3.5.1-bin-hadoop3/conf && sed -i 's/rootLogger.level = info/rootLogger.level = error/' log4j2.properties.template && mv log4j2.properties.template log4j2.properties"
        container_id=$(docker ps -aqf "name=spark-master")
        docker exec -it "$container_name" /bin/bash -c "$container_command && ./start-worker.sh spark://$container_id:7077"
        docker exec -it "$container_name" /bin/bash -c "cd /opt/spark-3.5.1-bin-hadoop3/bin && ./spark-submit --master spark://$container_id:7077 /app/streaming.py"
    fi
}

# Build the Docker image
if ! docker image inspect spark-image > /dev/null 2>&1; then
    echo "Construyendo la imagen Docker..."
    docker build -t spark-image .
fi

# Stop the worker containers
for container_name in $(docker ps --format '{{.Names}}' | grep -E "^spark-worker"); do
    stop_container "$container_name"
done

echo " "
echo "El procesamiento de datos en streaming se está iniciando."
echo "Debe abrir otra terminal para introducir consultas SQL, para ello, ejecuta: 'docker exec -it spark-master /bin/bash' y una vez dentro del contenedor ejecuta: 'nc -lk 9999'"
echo "Introduce consultas SQL por la otra terminal y los resultados saldrán por aquí: "
echo " "

# Start the master container
start_container "spark-master" "cd /opt/spark-3.5.1-bin-hadoop3/sbin && ./start-master.sh"

# Stop the master container
stop_container "spark-master"