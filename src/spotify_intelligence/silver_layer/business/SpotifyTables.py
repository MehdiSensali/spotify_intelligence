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
        if self.table_name == "artist":
            return df.with_columns(
                pl.col("external_urls").struct.field("spotify").alias("external_urls"),
                pl.col("images")
                .map_elements(lambda l: [x["url"] for x in l])
                .alias("images"),
            )
        if self.table_name == "tracks":
            return df.with_columns(
                pl.col("album").struct.field("album_type").alias("album_type"),
                pl.col("artists")
                .map_elements(lambda l: [x["id"] for x in l])
                .alias("artists"),
            )
        if self.table_name == "albums":
            return df.with_columns(
                pl.col("external_urls").struct.field("spotify").alias("external_urls"),
                pl.col("artists")
                .map_elements(lambda l: [x["id"] for x in l])
                .alias("artists"),
            )
        return df

    def save_silver(self, df):
        self.logger.info(f"Saving prepared data to {self.write_path}")
        if self.table_format == "PARQUET":
            df.write_parquet(self.write_path, mkdir=True)
        elif self.table_format == "DELTA":
            df.write_delta(self.write_path, mode="overwrite", overwrite_schema=True)
        else:
            raise ValueError(f"Unsupported format: {self.table_format}")
        self.logger.info(
            f"Saved {df.shape[0]} rows and {df.shape[1]} columns to {self.write_path}"
        )
