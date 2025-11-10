from spotify_intelligence.bronze_layer.RawData import RawData
from spotify_intelligence.silver_layer.PrepareData import PrepareData
import argparse
import os
import json
import importlib

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--runner", type=str, required=True, help="Specify the runner to execute"
    )
    args = parser.parse_args()
    runner_path = args.runner
    with open(runner_path, "r", encoding="utf-8") as f:
        runner: dict = json.load(f)

    module_name: str = runner["module"]
    python_module, class_name = module_name.rsplit(".", 1)
    module = importlib.import_module(python_module)
    cl = getattr(module, class_name)
    if issubclass(cl, RawData):
        instance: RawData = cl()
        instance.run()
    elif issubclass(cl, PrepareData):
        pass
