from dataclasses import dataclass
import os
import polars as pl
from spotify_intelligence.silver_layer.RawFactory import RawFactory
from spotify_intelligence.silver_layer.RawSource import RawSource, RawTable
import spotify_intelligence.Utils as Utils


class PrepareData:

    def __init__(self, source_name: str, table_name: str):
        self.source_name = source_name.lower()
        if table_name is None:
            raise ValueError("Table must be defined for PrepareData.")
        self.source: RawSource = RawFactory().get_source(source_name)
        self.table: RawTable = self.source.get_table(table_name)
        self.silver_rule_applyer = self.table.silver_rule_applyer
        self.full_path = os.path.join(
            self.source.source_path, source_name, self.table.relative_path
        )
        self.output_path = os.path.join(
            self.source.source_path, source_name, "data", self.table.relative_path
        )
        self.logger = Utils.setup_logger(name=f"{source_name}_{table_name}")

    def read_raw_data(self):
        self.logger.info(f"Reading raw data from {self.full_path}")
        if self.table.format == "parquet":
            df = pl.read_parquet(self.full_path)
        elif self.table.format == "delta":
            df = pl.read_delta(self.full_path)
        else:
            raise ValueError(f"Unsupported format: {self.table.format}")

        self.logger.info(
            f"Read {df.shape[0]} rows and {df.shape[1]} columns from {self.full_path}"
        )
        return df

    def prepare_table(self, df: pl.DataFrame) -> pl.DataFrame:
        raise NotImplementedError("Subclasses must implement prepare_table method.")

    def save_prepared_data(self, df: pl.DataFrame):
        self.logger.info(f"Saving prepared data to {self.output_path}")
        os.makedirs(os.path.dirname(self.output_path), exist_ok=True)
        if self.table.format == "parquet":
            df.write_parquet(self.output_path)
        elif self.table.format == "delta":
            df.write_delta(self.output_path)
        else:
            raise ValueError(f"Unsupported format: {self.table.format}")
        self.logger.info(
            f"Saved {df.shape[0]} rows and {df.shape[1]} columns to {self.output_path}"
        )

    def run(self):
        df_raw = self.read_raw_data()
        df_prepared = self.prepare_table(df_raw)
        self.save_prepared_data(df_prepared)
        self.logger.info(
            f"Data preparation completed for {self.source_name} - {self.table.name}"
        )
