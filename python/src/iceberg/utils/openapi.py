import os
from functools import lru_cache

import yaml

from iceberg import assets


@lru_cache(maxsize=1)
def read_yaml_file(path: str):
    return yaml.safe_load(open(path))


def load_openapi_spec():
    return read_yaml_file(os.path.join(list(assets.__path__)[0], "rest-catalog-open-api.yaml"))
