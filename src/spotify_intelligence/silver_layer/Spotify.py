from spotify_intelligence.silver_layer.RawSource import RawSource
from spotify_intelligence.silver_layer.SpotifyTables import SpotifyTables
import os


class Spotify(RawSource):
    source_name = "spotify"
    source_path = "/mnt/c/data_projects/lake"
    RawTables = {
        "artist": SpotifyTables(
            read_path=os.path.join(
                source_path, source_name, "artist", "raw", "artist.DELTA"
            ),
            write_path=os.path.join(
                source_path, source_name, "artist", "data", "artist.DELTA"
            ),
            source_name="spotify",
            table_name="artist",
            table_format="DELTA",
            partitioning=[],
        ),
        "album": SpotifyTables(
            read_path=os.path.join(
                source_path, source_name, "album", "raw", "album.DELTA"
            ),
            write_path=os.path.join(
                source_path, source_name, "album", "data", "album.DELTA"
            ),
            source_name="spotify",
            table_name="album",
            table_format="DELTA",
            partitioning=[],
        ),
        "track": SpotifyTables(
            read_path=os.path.join(
                source_path, source_name, "track", "raw", "track.DELTA"
            ),
            write_path=os.path.join(
                source_path, source_name, "track", "data", "track.DELTA"
            ),
            source_name="spotify",
            table_name="track",
            table_format="DELTA",
            partitioning=[],
        ),
    }
