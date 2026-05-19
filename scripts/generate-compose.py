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

def build_gateway(cfg):
    gateway_port = cfg['gateway']['port']
    amqp_port = cfg['rabbitmq']['amqp_port']
    
    return {
        'build': {
            'context': '.',
            'dockerfile': 'docker/gateway.Dockerfile'
        },
        'environment': [
            'SERVER_HOST=0.0.0.0',
            f'SERVER_PORT={gateway_port}',
            'MOM_HOST=rabbitmq',
            f'MOM_PORT={amqp_port}'
        ],
        'ports': [f"{gateway_port}:{gateway_port}"],
        'depends_on': {
            'rabbitmq': {'condition': 'service_healthy'}
        },
        'networks': ['money_laundering_network']
    }

def build_client(cfg):
    gateway_port = cfg['gateway']['port']
    
    return {
        'build': {
            'context': '.',
            'dockerfile': 'docker/client.Dockerfile'
        },
        'environment': [
            'SERVER_HOST=gateway',
            f'SERVER_PORT={gateway_port}',
            'INPUT_FILE=/data/dataset.csv'
        ],
        'depends_on': ['gateway'],
        'networks': ['money_laundering_network']
    }

def build_worker(svc_name, worker_type, cfg):
    amqp_port = cfg['rabbitmq']['amqp_port']
    
    env = [
        f"WORKER_TYPE={worker_type}",
        "MOM_HOST=rabbitmq",
        f"MOM_PORT={amqp_port}"
    ]
    
    if svc_name == 'bank_router':
        max_bank_count = cfg.get('services', {}).get('max_bank', {}).get('count', 1)
        env.append(f"MAX_BANK_WORKER_AMOUNT={max_bank_count}")
    
    if svc_name == 'period_filter':
        period_filter_cfg = cfg.get('services', {}).get('period_filter', {})

        env.extend([
            f"USD_INPUT_QUEUE_NAME={period_filter_cfg.get('usd_input_queue')}",
            f"RAW_INPUT_QUEUE_NAME={period_filter_cfg.get('raw_input_queue')}",

            f"GROUP_BY_ORIGIN_QUEUE_NAME={period_filter_cfg.get('group_by_origin_queue')}",
            f"GROUP_BY_DESTINATION_QUEUE_NAME={period_filter_cfg.get('group_by_destination_queue')}",

            f"PAYMENT_TYPE_FILTER_QUEUE_NAME={period_filter_cfg.get('payment_type_queue')}",

            f"AVG_BY_TYPE_PERIOD_1_QUEUE_NAME={period_filter_cfg.get('avg_by_type_period_1_queue')}",
            f"AVG_BY_TYPE_PERIOD_2_QUEUE_NAME={period_filter_cfg.get('avg_by_type_period_2_queue')}",

            f"AVG_BY_TYPE_PERIOD_1_START={period_filter_cfg.get('avg_by_type_period_1_start')}",
            f"AVG_BY_TYPE_PERIOD_1_END={period_filter_cfg.get('avg_by_type_period_1_end')}",

            f"AVG_BY_TYPE_PERIOD_2_START={period_filter_cfg.get('avg_by_type_period_2_start')}",
            f"AVG_BY_TYPE_PERIOD_2_END={period_filter_cfg.get('avg_by_type_period_2_end')}",

            f"SCATTER_GATHER_PERIOD_START={period_filter_cfg.get('scatter_gather_period_start')}",
            f"SCATTER_GATHER_PERIOD_END={period_filter_cfg.get('scatter_gather_period_end')}",

            f"PAYMENT_TYPE_PERIOD_START={period_filter_cfg.get('payment_type_period_start')}",
            f"PAYMENT_TYPE_PERIOD_END={period_filter_cfg.get('payment_type_period_end')}",
        ])

    return {
        'build': {
            'context': '.',
            'dockerfile': 'docker/worker.Dockerfile'
        },
        'environment': env,
        'depends_on': {
            'rabbitmq': {'condition': 'service_healthy'}
        },
        'networks': ['money_laundering_network']
    }

def generate_compose(cfg):
    compose = {
        'networks': {
            'money_laundering_network': {'driver': 'bridge'}
        },
        'services': {}
    }
    
    compose['services']['rabbitmq'] = build_rabbitmq(cfg)
    compose['services']['gateway'] = build_gateway(cfg)
    
    client_count = cfg.get('services', {}).get('client', {}).get('count', 0)
    for i in range(client_count):
        svc_id = f"client_{i}"
        compose['services'][svc_id] = build_client(cfg)
        
    workers_config = {
        'currency_filter': 'CURRENCY_FILTER',
        'max_bank': 'MAX_BANK',
        'bank_router': 'BANK_ROUTER',
        'period_filter': 'PERIOD_FILTER'
    }
    
    for svc_name, worker_type in workers_config.items():
        count = cfg.get('services', {}).get(svc_name, {}).get('count', 0)
        for i in range(count):
            svc_id = f"{svc_name}_{i}"
            compose['services'][svc_id] = build_worker(svc_name, worker_type, cfg)
            
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
