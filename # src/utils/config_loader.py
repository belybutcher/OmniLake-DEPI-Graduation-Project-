
import os
import yaml

def load_config(config_path: str = "config/config.yaml") -> dict:
    """
    Loads the central configuration YAML file for the OmniLake pipeline.
    """
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found at: {config_path}")
    
    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    return config

def get_storage_path(layer: str) -> str:
    """
    Resolves storage paths dynamically based on the environment (local vs. azure).
    Layers: 'bronze', 'silver', 'gold'
    """
    config = load_config()
    env = config.get("project", {}).get("environment", "local")
    
    if env == "local":
        return config["storage"]["local"][layer]
    elif env == "azure":
        return config["storage"]["azure"][layer]
    else:
        raise ValueError(f"Unknown environment specified in config: {env}")
