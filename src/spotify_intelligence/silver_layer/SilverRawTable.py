import abc
import polars as pl
import spotify_intelligence.Utils as Utils


class SilverRawTable(abc.ABC):
    def __init__(
        self,
        read_path: str,
        write_path: str,
        source_name: str,
        table_name: str,
        table_format: str = "DELTA",
        partitioning: list[str] = [],
    ):
        self.read_path = read_path
        self.write_path = write_path
        self.source_name = source_name
        self.table_name = table_name
        self.table_format = table_format
        self.partitioning = partitioning
        self.logger = Utils.DataLogger().setup_logger(name="PrepareData")

    def read_raw(self) -> pl.DataFrame:
        raise NotImplementedError("Subclasses must implement read_raw method.")

    def apply_rules(self, df: pl.DataFrame) -> pl.DataFrame:
        raise NotImplementedError("Subclasses must implement apply_rules method.")

    def save_silver(self, df: pl.DataFrame) -> None:
        raise NotImplementedError("Subclasses must implement save_silver method.")
