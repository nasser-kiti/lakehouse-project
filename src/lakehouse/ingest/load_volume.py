from pathlib import Path

from lakehouse.utils.source_config import ConfigManager
from lakehouse.utils.logger import get_logger


logger = get_logger('Landing Layer')
CONFIG_PATH = Path(__file__).with_name("data_sources.yaml")

class DataLoader:
    def __init__(self):
        self.sources = self.get_sources()

    def get_sources(self):
        src_config_manager = ConfigManager(CONFIG_PATH)
        sources = src_config_manager.get_configs()
        if not sources:
            logger.warning("No sources found in configuration.")
            return []

        logger.info(f"Retrieved {len(sources)} sources from configuration: ({', '.join([source.name for source in sources])})")
        return sources

    def upload_to_volume(self):
        for source in self.sources:
            logger.info(f"Processing {source.name} data")
            if not source.files:
                logger.warning(f"No files to upload for source {source.name}.")
                continue
            logger.info(f"Uploading {len(source.files)} files for source {source.name} to data lake.")
            for file in source.files:
                logger.info(f"Uploading {source.local_dir}/{file.file} -> bronze.{file.table}")
            # TO DO: Create a folder for each source and upload the files into the respective folder


if __name__ == "__main__":
    loader = DataLoader()
    loader.upload_to_volume()

