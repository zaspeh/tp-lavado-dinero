import argparse
import random
import re
import subprocess
import time

STATEFUL_TYPES = {
   "microtransaction_join",
   "max_bank",
   "max_bank_join",
   "avg_by_type",
   "avg_by_type_join",
   "group_by_origin",
   "group_by_destination",
   "aggregate_by_intermediary",
   "scatter_gather_join",
   "converter_join",
}

STATELESS_TYPES = {
    "currency_filter",
    "microtransaction_filter",
    "microtransaction_router_to_join",
    "bank_router",
    "max_bank_router_to_join",
    "period_filter",
    "payment_type_router",
    "avg_by_type_router_to_join",
    "origin_destination_router",
    "origin_intermediary_router",
    "destination_intermediary_router",
    "suspicious_path_router_to_join",
    "payment_format_filter",
    "currency_converter",
    "amount_converted_filter",
    "converted_amount_router_to_join",
}

TARGET_GROUPS = {
    "stateful": STATEFUL_TYPES,
    "stateless": STATELESS_TYPES,
}

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

def docker_command(args, hypervisor_container):
    if hypervisor_container:
        return ["docker", "exec", hypervisor_container, "docker"] + args

    return ["docker"] + args

def is_excluded(name):
    return (
        name.startswith("client_")
        or name == "gateway"
        or name == "rabbitmq"
        or name == "fault_hypervisor"
    )

def worker_type(name):
    return re.sub(r"_\d+$", "", name)

def get_containers(hypervisor_container):
    output = run(docker_command([
        "ps",
        "--format",
        "{{.Names}}",
    ], hypervisor_container))

    if not output:
        return []

    return [
        name
        for name in output.splitlines()
        if not is_excluded(name)
    ]

def target_types(target):
    if target is None:
        return None

    if target in TARGET_GROUPS:
        return TARGET_GROUPS[target]

    return {target}

def filter_containers(containers, target):
    allowed_types = target_types(target)

    if allowed_types is None:
        return containers

    return [
        name
        for name in containers
        if worker_type(name) in allowed_types
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
        if len(containers) > 1
    ]

    if not eligible_types:
        return None

    kind = random.choice(eligible_types)
    return random.choice(groups[kind])

def kill_container(name, hypervisor_container):
    run(docker_command([
        "kill",
        name,
    ], hypervisor_container))

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

    parser.add_argument(
        "--hypervisor-container",
        default="fault_hypervisor_2",
        help="Contenedor que corre Docker-in-Docker.",
    )

    parser.add_argument(
        "--target",
        help=(
            "Objetivo a matar. Puede ser 'stateful', 'stateless' "
            "o un tipo concreto de worker."
        ),
    )

    args = parser.parse_args()

    while True:
        try:
            containers = get_containers(args.hypervisor_container)
            containers = filter_containers(containers, args.target)

            groups = group_by_type(containers)
            victim = choose_victim(groups)

            if victim is None:
                print("No hay contenedores elegibles para matar.")
            elif args.kill:
                print(f"Matando contenedor: {victim}")
                kill_container(victim, args.hypervisor_container)
            else:
                print(f"[SIMULADO] Hubiera matado el contenedor: {victim}")

        except Exception as error:
            print(f"[ERROR] {error}")

        time.sleep(args.interval)

if __name__ == "__main__":
    main()