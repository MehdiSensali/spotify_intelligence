from spotify_intelligence.silver_layer.generics.RawFactory import RawFactory
from spotify_intelligence.silver_layer.generics.RawSource import (
    RawSource,
    SilverRawTable,
)

import spotify_intelligence.Utils as Utils


class PrepareData:

    def __init__(self, source_name: str, table_name: str):
        self.source_name = source_name.lower()
        if table_name is None:
            raise ValueError("Table must be defined for PrepareData.")
        self.table_name = table_name
        self.source: RawSource = RawFactory().get_source(source_name)
        self.silver_applyer: SilverRawTable = self.source.get_table(table_name)
        self.logger = Utils.DataLogger().setup_logger(name="PrepareData")

    def run(self):
        self.logger.info(
            f"Starting data preparation for {self.source_name} - {self.table_name}"
        )
        df_raw = self.silver_applyer.read_raw()
        df_prepared = self.silver_applyer.apply_rules(df_raw)
        self.silver_applyer.save_silver(df_prepared)
        self.logger.info(
            f"Data preparation completed for {self.source_name} - {self.table_name}"
        )
