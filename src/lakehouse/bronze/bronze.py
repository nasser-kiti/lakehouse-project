import os
from pyspark.sql import SparkSession
from utils.source_config import ConfigManager
from utils.logger import get_logger


logger = get_logger('Bronze Layer')
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

class BronzeLayer:
    def __init__(self):
        #  get a list of source names
        self.sources = []
        # self.spark = SparkSession.builder.appName("BronzeLayer").getOrCreate()

    def get_sources(self):
        src_config_manager = ConfigManager(ROOT_DIR)
        self.sources = src_config_manager.get_config()

    def read_source_data(self):
        logger.info(f"Starting to read source data for {len(self.sources)} sources.")

        for source in self.sources:
            logger.info(f"Processing {source.name} data")
            for file_path in source.data:
                logger.info(f"Reading file: {file_path}")
                # df = (
                #     self.spark.read.option("header", "true")\
                #     .option("inferSchema", "true")\
                #     .csv(file_path)
                # )
                # df.show()
                # df.write.mode("overwrite").parquet(f"bronze/{source.name}/{os.path.basename(file_path)}.parquet")



if __name__ == "__main__":
    bronze_layer = BronzeLayer()
    bronze_layer.get_sources()
    bronze_layer.read_source_data()

