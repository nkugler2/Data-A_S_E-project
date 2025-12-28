# Provides a way to load the configuration for the sample data pipeline #
## This is a class that allows me to load the configuration file and use the properties to access the configuration ##
## Helps me use seperation of concerns ##

import yaml
from pathlib import Path
from typing import Dict, Any

class Config:
    """Configuration manager for SEC pipeline"""
    
    ## This is where I load the configuration file ##
    def __init__(self, config_path: str = "05_config/sampleDataConfig.yaml"):
        """Initializes the configuration manager"""
        self.config_path = Path(config_path)
        self.config = self._load_config()
        
    ## This is where I load the configuration file ##
    def _load_config(self) -> Dict[str, Any]:
        """Load YAML configuration"""
        with open(self.config_path, 'r') as f:
            return yaml.safe_load(f) # Safe load to avoid security issues
    
    # Properties for the configuration #

    # Checks if the pipeline is in sample mode #
    @property
    def is_sample_mode(self) -> bool:
        return self.config['pipeline']['mode'] == 'sample'
    
    # Gets the path for the bronze layer #
    @property
    def bronze_path(self) -> Path:
        return Path(self.config['data']['paths']['bronze'])
    
    # Gets the path for the silver layer #
    @property
    def silver_path(self) -> Path:
        return Path(self.config['data']['paths']['silver'])
    
    # Gets the path for the gold layer #
    @property
    def gold_path(self) -> Path:
        return Path(self.config['data']['paths']['gold'])
    
    # Gets the path for the database #
    @property
    def database_path(self) -> Path:
        return Path(self.config['data']['paths']['database'])
    
    # Gets the quarters for the data #
    @property
    def quarters(self) -> list:
        return self.config['data']['quarters']
