import os
from pathlib import Path
import json


class TableTracker:
    def __init__(self, read_path: str):
        self.tracker_path = os.path.join(Path(read_path).parent, "tracker")
        os.makedirs(self.tracker_path, exist_ok=True)
        self.tracker_file_path = os.path.join(self.tracker_path, "tracker.json")

    def get_processed_paths(self) -> list[str]:
        # Create file if it doesn't exist
        if not os.path.exists(self.tracker_file_path):
            with open(self.tracker_file_path, "w") as f:
                json.dump({"files": []}, f)

        # Now read it safely
        with open(self.tracker_file_path, "r") as f:
            data = json.load(f)

        return data.get("files", [])

    def save_processed_paths(self, partitions: list[str]):
        with open(self.tracker_file_path, "r+") as tracker_file:
            old_processed = list(json.load(tracker_file).get("files", []))
            processed = old_processed + list(set(partitions) - set(old_processed))
            tracker_file.seek(0)
            json.dump({"files": processed}, tracker_file)
            tracker_file.truncate()
