"""Loads and validates the source contract (data_sources.yaml).

Adding a source system or file is a config change, not a code change.
Validation fails fast at load time so a bad config never reaches the volume.
"""
from dataclasses import dataclass, field
from pathlib import Path

import yaml

from lakehouse.utils.logger import get_logger

logger = get_logger('Source Config')


@dataclass(frozen=True)
class SourceFile:
    file: str   # name exactly as delivered by the source system
    table: str  # target Bronze table


@dataclass
class SourceConfig:
    name: str
    local_dir: str
    files: list[SourceFile] = field(default_factory=list)


class ConfigManager:
    def __init__(self, config_path: str | Path):
        self.config_path = Path(config_path)
        self.config: dict = self.load_config()

    def load_config(self) -> dict:
        if not self.config_path.is_file():
            logger.error(f"Config file '{self.config_path}' does not exist.")
            raise FileNotFoundError(f"Config file '{self.config_path}' does not exist.")

        with self.config_path.open() as f:
            return yaml.safe_load(f) or {}

    def get_configs(self) -> list[SourceConfig]:
        '''Returns a validated list of configurations, one per source system.'''
        source_configs: list[SourceConfig] = []
        seen_tables: set[str] = set()

        for name, details in (self.config.get('sources') or {}).items():
            try:
                local_dir = details['local_dir']
                files = [SourceFile(file=f['file'], table=f['table']) for f in details['files']]
            except (KeyError, TypeError) as exc:
                raise ValueError(f"Source '{name}' is missing required key {exc} in {self.config_path}") from exc

            for f in files:
                if f.table in seen_tables:
                    raise ValueError(f"Bronze table '{f.table}' is configured more than once.")
                seen_tables.add(f.table)

            source_configs.append(SourceConfig(name=name, local_dir=local_dir, files=files))

        return source_configs
