import abc
import os
import polars as pl
import logging
from logging import Logger
import spotify_intelligence.Utils as Utils


class RawData(abc.ABC):
    base_raw_path = None
    source = None
    tables: list[str] = []

    def __init__(self):
        self.raw_path = os.path.join(self.base_raw_path, self.source)
        self.logger: Logger = Utils.setup_logger(name=self.source, level=logging.DEBUG)

    def setup(self):
        pass

    def collect_data(self, table: str):
        raise NotImplementedError

    def save_data(self, df: pl.DataFrame, table: str):
        raise NotImplementedError

    def prepare_data(self, df: pl.DataFrame, table: str) -> pl.DataFrame:
        return df

    def run(self):
        self.setup()
        for table in self.tables:
            self.logger.info(f"Collecting data for table: {table}")
            df_raw = self.collect_data(table)
            self.logger.info(f"Preparing data for table: {table}")
            df = self.prepare_data(df_raw, table)
            self.logger.info(f"Saving data for table: {table}")
            self.save_data(df, table)
            self.logger.info(f"Data collection and saving completed for table: {table}")
