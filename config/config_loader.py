import os 
import yaml 

# ---------------------------------- LOADING CONFIG ----------------------------------
default_config_path = os.path.join(os.path.dirname(__file__), 'config.yaml')

config = None

def load_config(path = None):
    global config

    if config is not None and path is not None:
        return config
    
    config_path = config or default_config_path

    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")
    
    with open(config_path, 'r') as f:
        cfg = yaml.safe_load(f)

    if path is not None:
        cfg = config
    
    return cfg


# ---------------------------------- LOADING METDATA ----------------------------------
default_metadata_path = os.path.join(os.path.dirname(__file__), 'tables_metadata.yaml')

metdata = None

def load_metadata(path = None):
    global metdata

    if metdata is not None and path is not None:
        return config
    
    metdata_path = metdata or default_metadata_path

    if not os.path.exists(metdata_path):
        raise FileNotFoundError(f"Metadata file not found: {metdata_path}")
    
    with open(metdata_path, 'r') as f:
        cfg = yaml.safe_load(f)

    if path is not None:
        cfg = metdata
    
    return cfg