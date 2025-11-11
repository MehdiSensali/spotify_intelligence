from spotify_intelligence.silver_layer.generics.SilverRawTable import SilverRawTable
import polars as pl


class SpotifyTables(SilverRawTable):
    def __init__(
        self,
        read_path: str,
        write_path: str,
        source_name: str,
        table_name: str,
        table_format: str = "DELTA",
        partitioning: list[str] = [],
    ):
        super().__init__(
            read_path, write_path, source_name, table_name, table_format, partitioning
        )

    def read_raw(self):
        self.logger.info(f"Reading raw data from {self.read_path}")
        if self.table_format == "PARQUET":
            df = pl.read_parquet(self.read_path)
        elif self.table_format == "DELTA":
            df = pl.read_delta(self.read_path)
        else:
            raise ValueError(f"Unsupported format: {self.table_format}")

        self.logger.info(
            f"Read {df.shape[0]} rows and {df.shape[1]} columns from {self.read_path}"
        )
        return df

    def apply_rules(self, df):
        return df

    def save_silver(self, df):
        self.logger.info(f"Saving prepared data to {self.write_path}")
        if self.table_format == "PARQUET":
            df.write_parquet(self.write_path, mkdir=True)
        elif self.table_format == "DELTA":
            df.write_delta(self.write_path, mode="overwrite")
        else:
            raise ValueError(f"Unsupported format: {self.table_format}")
        self.logger.info(
            f"Saved {df.shape[0]} rows and {df.shape[1]} columns to {self.write_path}"
        )
