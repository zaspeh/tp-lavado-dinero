#!/bin/sh

rm -f /tmp/ready
rm -f /var/run/docker.pid /var/run/docker.sock

dockerd >/tmp/dockerd.log 2>&1 &
dockerd_pid="$!"

echo "Waiting for Docker daemon..."

until docker info >/dev/null 2>&1
do
    if ! kill -0 "$dockerd_pid" >/dev/null 2>&1; then
        echo "Docker daemon failed to start"
        cat /tmp/dockerd.log
        exit 1
    fi
    sleep 1
done

echo "Docker daemon ready"

exec /app/fault_hypervisor
