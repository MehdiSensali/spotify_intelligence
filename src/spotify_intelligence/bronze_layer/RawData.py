import abc
import os
import polars as pl


@abc.abstractmethod
class RawDataCollector:
    base_raw_path = "/mnt/c/data_projects/lake"
    source = None
    tables: list[str] = []

    def __init__(self):
        self.raw_path = os.path.join(self.base_raw_path, self.source)
        self.query = "2023-2026"
        self.limit = 50

    def collect_data(self, table: str):
        raise NotImplementedError

    def save_data(self, df: pl.DataFrame, table: str):
        raise NotImplementedError

    def prepare_data(self, df: pl.DataFrame, table: str) -> pl.DataFrame:
        return df

    def run(self):
        for table in self.tables:
            print(f"Collecting data for table: {table}")
            df = self.collect_data(table)
            print(f"Saving data for table: {table}")
            self.save_data(df, table)
            print(f"Data collection and saving completed for table: {table}")
