from spotify_intelligence.silver_layer.generics.RawSource import RawSource
from spotify_intelligence.silver_layer.business.SpotifyTables import SpotifyTables
import os


class Spotify(RawSource):
    source_name = "spotify"
    source_path = "/mnt/c/data_projects/lake"
    RawTables = {
        "artist": SpotifyTables(
            source_name="spotify",
            table_name="artist",
            table_format="JSON",
            partitioning=["year", "month", "day", "hour"],
        ),
        "album": SpotifyTables(
            source_name="spotify",
            table_name="album",
            table_format="JSON",
            partitioning=["year", "month", "day", "hour"],
        ),
        "track": SpotifyTables(
            source_name="spotify",
            table_name="track",
            table_format="JSON",
            partitioning=["year", "month", "day", "hour"],
        ),
    }
