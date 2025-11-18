import logging
import logging.config
import yaml
from pathlib import Path

def setup_logging(config_path: str = 'config/config.yaml'):
    """
    Setup logging from YAML configuration
    """
    config_file = Path(config_path)
    
    if config_file.exists():
        with open(config_file, 'r') as f:
            config = yaml.safe_load(f)
            if 'logging' in config:
                logging.config.dictConfig(config['logging'])
    else:
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )

def get_logger(name: str) -> logging.Logger:
    """Get a logger instance"""
    return logging.getLogger(name)