from spotify_intelligence.silver_layer.generics.SilverRawTable import SilverRawTable
from pathlib import Path
import polars as pl
import re


class SpotifyTables(SilverRawTable):
    def __init__(
        self,
        source_name: str,
        table_name: str,
        table_format: str,
        partitioning: list[str] = [],
    ):
        super().__init__(
            source_name=source_name,
            table_name=table_name,
            table_format=table_format,
            partitioning=partitioning,
        )

    def read_raw(self):
        self.logger.info(f"Reading raw data from {self.read_path}")
        processed_paths = self.tracker.get_processed_paths()
        if self.table_format == "JSON":

            def extract_partitions(path: str):
                return dict(re.findall(r"(\w+)=([\w-]+)", path))

            root = Path(self.read_path)
            partition_pattern = re.compile(
                r"(?:^|/)year=\d{4}(?:/month=\d{2})?(?:/day=\d{2})?"
            )
            # 1. Find only JSON files under valid partition paths
            paths = [
                str(p)
                for p in root.rglob("*.json")
                if (partition_pattern.search(str(p)))
                and (str(p) not in processed_paths)
            ]

            df_parts: list[pl.DataFrame] = []
            for path in paths:
                parts = extract_partitions(path)
                lf = pl.read_json(path)
                for key, value in parts.items():
                    lf = lf.with_columns(pl.lit(value).alias(key))
                df_parts.append(lf)

            df = pl.concat(df_parts)
            self.tracker.save_processed_paths(partitions=paths)
        elif self.table_format == "PARQUET":
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
        df.write_parquet(self.write_path, partition_by=self.partitioning)
        self.logger.info(
            f"Saved {df.shape[0]} rows and {df.shape[1]} columns to {self.write_path}"
        )
