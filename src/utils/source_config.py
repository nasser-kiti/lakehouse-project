import os
import yaml
from typing import List
from utils.logger import get_logger

logger = get_logger('Source Config')


class SourceConfig:
    name: str = ''
    data_path: str = ''
    data: List[str] = []


class ConfigManager:
    def __init__(self, config_dir):
        self.config_dir = config_dir
        self.config: dict = {}
        self.load_config()

    def load_config(self):
        if not os.path.exists(self.config_dir):
            logger.error(f"Config directory '{self.config_dir}' does not exist.")
            raise FileNotFoundError(f"Config directory '{self.config_dir}' does not exist.")

        config_path = os.path.join(self.config_dir, "sources.yaml")
        if os.path.exists(config_path):
            self.config = yaml.safe_load(open(config_path))
            logger.info(f"Loaded config: {self.config}")
        else:
            logger.error(f"Config file '{config_path}' does not exist.")
            raise FileNotFoundError(f"Config file '{config_path}' does not exist.")

    def get_config(self) -> List[SourceConfig]:
        source_configs: List[SourceConfig] = []
        sources = self.config.get('sources', {})

        for source, details in sources.items():
            source_config = SourceConfig()
            source_config.name = source
            source_config.data = [str(f) for f in details]
            source_configs.append(source_config)
        return source_configs
