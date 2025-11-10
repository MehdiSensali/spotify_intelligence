from spotify_intelligence.silver_layer.RawSource import RawSource, RawTable


class Spotify(RawSource):
    source_name = "spotify"
    source_path = "/mnt/c/data_projects/lake"
    RawTable = {
        "artist": RawTable(
            relative_path="artist/raw/album.DELTA",
            format="delta",
            partitioning=[],
        ),
        "album": RawTable(
            relative_path="album/raw/album.DELTA",
            format="delta",
            partitioning=[],
        ),
        "track": RawTable(
            relative_path="track/raw/track.DELTA",
            format="delta",
            partitioning=[],
        ),
    }
