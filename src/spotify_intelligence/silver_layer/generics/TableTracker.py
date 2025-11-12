from spotify_intelligence.silver_layer.generics.SilverRawTable import SilverRawTable
class TableTracker:

    def __init__(self, table: SilverRawTable):
        self.table = table
        
    def get_processed_paths(self) -> list[str]:
        pass
    def save_processed_paths(self):
        pass
