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

delete_container() {
    local container_name=$1
    if docker ps -a --format '{{.Names}}' | grep -Eq "^$container_name\$"; then
        echo "Borrando contenedor $container_name..."
        docker rm "$container_name"
    else
        echo "El contenedor $container_name no existe"
    fi
}

# Stop the worker containers
for container_name in $(docker ps --format '{{.Names}}' | grep -E "^spark-worker"); do
    stop_container "$container_name"
done

# Stop the master container
stop_container "spark-master"

# Ask to delete containers
read -p "¿Quieres borrar los contenedores creados para esta aplicación? (y/n) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]
then
    for container_name in $(docker ps -a --format '{{.Names}}' | grep -E "^spark-worker"); do
        delete_container "$container_name"
    done

    delete_container "spark-master"
    # Ask to delete spark image
    read -p "¿Quieres borrar la imagen de Docker creada para esta aplicación (spark-image)? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]
    then
        docker rmi spark-image
    fi
fi