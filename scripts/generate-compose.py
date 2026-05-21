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

def build_client(cfg, i):
    gateway_port = cfg['gateway']['port']
    client_cfg = cfg['services']['client']
    
    host_datasets = client_cfg.get('datasets_dir', './datasets')
    host_output_base = client_cfg.get('output_dir_base', './outputs')

    return {
        'build': {
            'context': '.',
            'dockerfile': 'docker/client.Dockerfile'
        },
        'environment': [
            f'ID={i}',
            'SERVER_HOST=gateway',
            f'SERVER_PORT={gateway_port}',
            f'INPUT_FILE_TRANSACTIONS=/datasets/client_{i}_transactions.csv',
            f'INPUT_FILE_ACCOUNTS=/datasets/client_{i}_accounts.csv',
            'OUTPUT_DIR=/outputs' 
        ],
        'volumes': [
            f'{host_datasets}:/datasets',
            f'{host_output_base}/client_{i}:/outputs'
        ],
        'depends_on': ['gateway'],
        'networks': ['money_laundering_network']
    }

def build_worker(svc_name, cfg, i):
    amqp_port = cfg['rabbitmq']['amqp_port']
    svc_config = cfg['services'][svc_name]
    worker_type = svc_config.get('worker_type', 'UNKNOWN')
    
    env_list = [
        f"ID={i}",
        f"WORKER_TYPE={worker_type}",
        "MOM_HOST=rabbitmq",
        f"MOM_PORT={amqp_port}"
    ]
    
    if 'env' in svc_config:
        for key, value in svc_config['env'].items():
            env_list.append(f"{key}={value}")

    return {
        'build': {
            'context': '.',
            'dockerfile': 'docker/worker.Dockerfile'
        },
        'environment': env_list,
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
    
    for svc_name, svc_data in cfg.get('services', {}).items():
        count = svc_data.get('count', 0)
        
        if svc_name == 'client':
            for i in range(count):
                compose['services'][f"client_{i}"] = build_client(cfg, i)
        else:
            for i in range(count):
                compose['services'][f"{svc_name}_{i}"] = build_worker(svc_name, cfg, i)
            
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