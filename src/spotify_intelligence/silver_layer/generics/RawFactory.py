from spotify_intelligence.silver_layer.generics.RawSource import RawSource
import importlib


class RawFactory:

    def get_source(self, source_name: str) -> RawSource:
        raw_source_module = importlib.import_module(
            f"spotify_intelligence.silver_layer.business.{source_name}"
        )
        raw_source_class = getattr(raw_source_module, source_name)
        for cls in RawSource.__subclasses__():
            if cls.__name__ == raw_source_class.__name__:
                return cls(source_name=source_name)
        raise ValueError(f"RawSource with name {source_name} not found.")
