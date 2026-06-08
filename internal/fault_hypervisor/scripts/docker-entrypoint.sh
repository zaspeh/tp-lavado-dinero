#!/bin/sh

dockerd >/tmp/dockerd.log 2>&1 &

echo "Waiting for Docker daemon..."

until docker info >/dev/null 2>&1
do
    sleep 1
done

echo "Docker daemon ready"

exec /app/fault_hypervisor