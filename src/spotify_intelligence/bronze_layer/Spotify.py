from spotify_intelligence.bronze_layer.RawData import RawDataCollector
from spotify_intelligence.Utils import utils
from spotipy import Spotify as SpotipyClient, SpotifyException
import os
import polars as pl


class Spotify(RawDataCollector):
    source = "spotify"
    tables = ["artist", "album", "track"]

    def setup(self):
        TOKEN: str = utils.get_bearer_token(
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
                    print(f"SpotifyException at offset {offset}: {e}")
                    TOKEN: str = utils.get_bearer_token(
                        client_id=os.getenv("SPOTIFY_CLIENT_ID"),
                        client_secret=os.getenv("SPOTIFY_CLIENT_SECRET"),
                    )
                    spotify_client: Spotify = Spotify(auth=TOKEN)
                    result = spotify_client.search(
                        q=self.query, type=table, limit=self.limit, offset=offset
                    )
                except Exception as e:
                    print(f"Error fetching artists at offset {offset}: {e}")
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

        # sanitize DataFrame to avoid unsupported dtypes (e.g. pl.Null)

        df.write_delta(file_path, mode="overwrite")

    def run(self):
        self.setup()
        for table in self.tables:
            print(f"Collecting data for table: {table}")
            df_raw = self.collect_data(table)
            print(f"Preparing data for table: {table}")
            df = self.prepare_data(df_raw, table)
            print(f"Saving data for table: {table}")
            self.save_data(df, table)
            print(f"Data collection and saving completed for table: {table}")
