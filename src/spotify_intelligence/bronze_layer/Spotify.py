from spotify_intelligence.bronze_layer.RawData import RawDataCollector
import spotify_intelligence.Utils as Utils
from spotipy import Spotify as SpotipyClient, SpotifyException
import os
import polars as pl


class Spotify(RawDataCollector):
    base_raw_path = "/mnt/c/data_projects/lake"
    source = "spotify"
    tables = ["artist", "album", "track"]

    def __init__(self, query: str = "2023-2026", limit: int = 50):
        super().__init__()
        self.query = query
        self.limit = limit

    def setup(self):
        TOKEN: str = Utils.get_bearer_token(
            client_id=os.getenv("SPOTIFY_CLIENT_ID"),
            client_secret=os.getenv("SPOTIFY_CLIENT_SECRET"),
        )
        self.spotify_client: SpotipyClient = SpotipyClient(auth=TOKEN)

    def collect_data(self, table: str):
        offset_list = [
            self.limit * i - 1 if i > 0 else 0 for i in range(1000 // self.limit)
        ]

        results = []
        for offset in offset_list:
            try:
                result = self.spotify_client.search(
                    q=self.query, type=table, limit=self.limit, offset=offset
                )
                results.extend(result[f"{table}s"]["items"])
            except SpotifyException as e:
                try:
                    self.logger.error(f"SpotifyException at offset {offset}: {e}")
                    TOKEN: str = Utils.get_bearer_token(
                        client_id=os.getenv("SPOTIFY_CLIENT_ID"),
                        client_secret=os.getenv("SPOTIFY_CLIENT_SECRET"),
                    )
                    spotify_client: Spotify = Spotify(auth=TOKEN)
                    result = spotify_client.search(
                        q=self.query, type=table, limit=self.limit, offset=offset
                    )
                except Exception as e:
                    self.logger.error(f"Error fetching artists at offset {offset}: {e}")
                    raise e
                finally:
                    results.extend(result[f"{table}s"]["items"])
        return pl.DataFrame(results)

    def prepare_data(self, df: pl.DataFrame, table: str) -> pl.DataFrame:
        if table == "artist":
            return df.with_columns(
                pl.col("followers")
                .map_elements(lambda x: x["total"])
                .alias("followers")
            )
        if table == "track":
            return df.select(
                [
                    (
                        pl.col(col_name).cast(pl.Int8)
                        if df.schema[col_name] == pl.Null
                        else pl.col(col_name)
                    )
                    for col_name in df.columns
                ]
            )
        return df

    def save_data(self, df: pl.DataFrame, table: str):
        table_path = os.path.join(self.raw_path, table)
        os.makedirs(table_path, exist_ok=True)
        file_path = os.path.join(table_path, f"{table}_data.delta")
        df.write_delta(file_path, mode="overwrite")
