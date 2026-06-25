import sys
try:
    import yaml
except ImportError:
    print("Error: PyYAML required. Install with: pip install pyyaml")
    sys.exit(1)

def load_config(file_path="config.yml"):
    with open(file_path, 'r') as f:
        return yaml.safe_load(f)

def build_rabbitmq(cfg):
    amqp_port = cfg['rabbitmq']['amqp_port']
    management_port = cfg['rabbitmq']['management_port']
    
    return {
        'image': 'rabbitmq:3-management',
        'container_name': 'rabbitmq',
        'environment': ['RABBITMQ_LOG_LEVELS=error'],
        'healthcheck': {
            'interval': '5s',
            'retries': 10,
            'start_period': '50s',
            'test': 'rabbitmq-diagnostics check_port_connectivity',
            'timeout': '3s'
        },
        'ports': [
            f"{management_port}:{management_port}",
            f"{amqp_port}:{amqp_port}"
        ],
        'networks': ['money_laundering_network']
    }

def build_gateway(cfg, log_level):
    gateway_port = cfg['gateway']['port']
    amqp_port = cfg['rabbitmq']['amqp_port']
    output_queue_name = cfg['gateway']['env'].get('OUTPUT_QUEUE_NAME')
    client_exchange_name = cfg['gateway']['env'].get('CLIENT_EXCHANGE_NAME')
    raw_input_queue_name = cfg['gateway']['env'].get('RAW_DATA_QUEUE_NAME')
    max_bank_router_queue_name = cfg['gateway']['env'].get('MAX_BANK_ROUTER_QUEUE_NAME')
    converter_join_amount = cfg['gateway']['env'].get('CONVERTER_JOIN_AMOUNT')
    return {
        'build': {
            'context': '.',
            'dockerfile': 'cmd/gateway/Dockerfile'
        },
        'container_name': 'gateway',
        'environment': [
            'SERVER_HOST=0.0.0.0',
            f'SERVER_PORT={gateway_port}',
            'MOM_HOST=rabbitmq',
            f'MOM_PORT={amqp_port}',
            f'OUTPUT_QUEUE_NAME={output_queue_name}',
            f'CLIENT_EXCHANGE_NAME={client_exchange_name}',
            f'RAW_DATA_QUEUE_NAME={raw_input_queue_name}',
            f'MAX_BANK_ROUTER_QUEUE_NAME={max_bank_router_queue_name}',
            f'LOG_LEVEL={log_level}',
            f'CONVERTER_JOIN_AMOUNT={converter_join_amount}'
        ],
        'ports': [f"{gateway_port}:{gateway_port}"],
        'depends_on': {
            'rabbitmq': {'condition': 'service_healthy'}
        },
        'networks': ['money_laundering_network']
    }

def build_client(cfg, i, log_level):
    gateway_port = cfg['gateway']['port']
    client_cfg = cfg['services']['client']
    
    host_datasets = client_cfg.get('datasets_dir', './datasets')
    host_output_base = client_cfg.get('output_dir_base', './outputs')
    max_batch_weight = client_cfg.get('max_batch_weight', 8192)

    return {
        'build': {
            'context': '.',
            'dockerfile': 'cmd/client/Dockerfile'
        },
        'container_name': f'client_{i}',
        'environment': [
            f'ID={i}',
            'SERVER_HOST=gateway',
            f'SERVER_PORT={gateway_port}',
            f'INPUT_FILE_TRANSACTIONS=/datasets/client_{1}_transactions.csv',
            f'INPUT_FILE_ACCOUNTS=/datasets/client_{1}_accounts.csv',
            'OUTPUT_DIR=/outputs',
            f'MAX_BATCH_WEIGHT={max_batch_weight}',
            f'LOG_LEVEL={log_level}'
        ],
        'volumes': [
            f'{host_datasets}:/datasets',
            f'{host_output_base}/client_{i}:/outputs'
        ],
        'depends_on': {
            'gateway': {
                'condition': 'service_started'
            },
            'fault_hypervisor': {
                'condition': 'service_healthy'
            }
        },
        'networks': ['money_laundering_network']
    }

def build_worker(svc_name, cfg, i, log_level):
    amqp_port = cfg['rabbitmq']['amqp_port']
    svc_config = cfg['services'][svc_name]
    worker_type = svc_config.get('worker_type', 'UNKNOWN')
    count = svc_config.get('count', 1)
    worker_exchange_name = svc_config.get('worker_exchange_name', worker_type)
    container_name = f"{svc_name}_{i}"

    env_list = [
        f"ID={i}",
        f"CONTAINER_NAME={container_name}",
        f"LOG_LEVEL={log_level}",
        f"WORKER_TYPE={worker_type}",
        "MOM_HOST=rabbitmq",
        f"MOM_PORT={amqp_port}",
        f"WORKER_COUNT={count}",
        f"WORKER_EXCHANGE_NAME={worker_exchange_name}"
    ]
        
    if 'env' in svc_config:
        for key, value in svc_config['env'].items():
            env_list.append(f"{key}={value}")

    return {
        'build': {
            'context': '.',
            'dockerfile': 'cmd/worker/Dockerfile'
        },
        'container_name': container_name,
        'environment': env_list,
        'depends_on': {
            'rabbitmq': {'condition': 'service_healthy'}
        },
        'networks': ['money_laundering_network']
    }


def build_fault_hypervisor(cfg):
    amqp_port = cfg['rabbitmq']['amqp_port']

    env_list = [
        "MOM_HOST=rabbitmq",
        f"MOM_PORT={amqp_port}",
    ]

    for key, value in cfg['fault_hypervisor']['env'].items():
        env_list.append(f"{key}={value}")

    return {
        'build': {
            'context': '.',
            'dockerfile': 'cmd/fault_hypervisor/Dockerfile'
        },
        'container_name': 'fault_hypervisor',
        'privileged': True,
        'environment': env_list,
        'healthcheck': {
            'test': ['CMD', 'test', '-f', '/tmp/ready'],
            'interval': '5s',
            'timeout': '3s',
            'retries': 40,
            'start_period': '10s'
        },
        'volumes': [
            './Compose.yml:/app/Compose.yml:ro',
            './config.yml:/app/config.yml:ro',
            '.:/workspace:ro'
        ],
        'depends_on': {
            'rabbitmq': {'condition': 'service_healthy'}
        },
        'networks': ['money_laundering_network']
    }

def generate_compose(cfg):
    global_log_level = cfg.get('global_log_level', 'info')

    compose = {
        'networks': {
            'money_laundering_network': {'driver': 'bridge'}
        },
        'services': {}
    }

    compose['services']['rabbitmq'] = build_rabbitmq(cfg)

    gateway_log_level = cfg['gateway'].get(
        'log_level',
        global_log_level,
    )

    compose['services']['gateway'] = build_gateway(
        cfg,
        gateway_log_level,
    )

    hypervisor_manages_workers = (
        cfg.get('fault_hypervisor', {})
           .get('manages_workers', False)
    )

    for svc_name, svc_data in cfg.get('services', {}).items():

        count = svc_data.get('count', 0)
        svc_log_level = svc_data.get(
            'log_level',
            global_log_level,
        )

        if svc_name == 'client':
            for i in range(count):
                compose['services'][f"client_{i}"] = build_client(
                    cfg,
                    i,
                    svc_log_level,
                )
            continue

        if hypervisor_manages_workers:
            continue

        for i in range(count):
            compose['services'][f"{svc_name}_{i}"] = build_worker(
                svc_name,
                cfg,
                i,
                svc_log_level,
            )

    if cfg.get('fault_hypervisor', {}).get('enabled', False):
        compose['services']['fault_hypervisor'] = build_fault_hypervisor(cfg)

    return compose

def main():
    try:
        cfg = load_config()
        compose = generate_compose(cfg)
        
        with open('Compose.yml', 'w') as f:
            yaml.dump(compose, f, default_flow_style=False, sort_keys=False)
            
        print("Generated Compose.yml successfully")

    except FileNotFoundError:
        print("Error: config.yml not found")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()