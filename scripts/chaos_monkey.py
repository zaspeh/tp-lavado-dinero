import argparse
import random
import re
import subprocess
import time

def run(command):
    result = subprocess.run(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip())

    return result.stdout.strip()

def is_excluded(name):
    return (
        name.startswith("client_")
        or name == "gateway"
        or name == "rabbitmq"
        # Por el momento evitamos matar al hypervisor
        or name == "fault_hypervisor"
    )

def worker_type(name):
    # Reemplaza el numero final por cadena vacia
    # Ejemplo: "avg_by_type_1" -> "avg_by_type"
    return re.sub(r"_\d+$", "", name)

def get_containers():
    output = run([
        "docker",
        "ps",
        "--format",
        "{{.Names}}",
    ])

    if not output:
        return []

    return [
        name
        for name in output.splitlines()
        if not is_excluded(name)
    ]

def group_by_type(containers):
    groups = {}

    for name in containers:
        kind = worker_type(name)
        groups.setdefault(kind, []).append(name)

    return groups

def choose_victim(groups):
    eligible_types = [
        kind
        for kind, containers in groups.items()

        # Aseguramos que haya al menos 2 contenedores de ese tipo para no matar el último
        if len(containers) > 1
    ]

    if not eligible_types:
        return None

    # Elegimos un tipo de contenedor entre todos
    kind = random.choice(eligible_types)

    # Elegimos un contenedor al azar dentro del tipo elegido
    return random.choice(groups[kind])

def kill_container(name):
    run(["docker", "kill", name])

def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--kill",
        action="store_true",
        help="Mata realmente el contenedor. Si no se indica, solo simula.",
    )

    parser.add_argument(
        "--interval",
        type=int,
        default=10,
        help="Tiempo en segundos entre cada intento.",
    )

    args = parser.parse_args()

    while True:
        containers = get_containers()
        groups = group_by_type(containers)
        victim = choose_victim(groups)

        if victim is None:
            print("No hay contenedores elegibles para matar.")
        elif args.kill:
            print(f"Matando contenedor: {victim}")
            kill_container(victim)
        else:
            print(f"[SIMULADO] Hubiera matado el contenedor: {victim}")

        time.sleep(args.interval)

if __name__ == "__main__":
    main()
