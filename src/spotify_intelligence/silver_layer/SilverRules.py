import abc
from spotify_intelligence.silver_layer.RawSource import RawTable
class SilverRules(abc.ABC):
     def __init__(self, raw : RawTable):
         self.raw = raw
    