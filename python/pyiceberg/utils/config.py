# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
import logging
import os
from typing import List, Optional

import yaml

from pyiceberg.typedef import FrozenDict, RecursiveDict

PYICEBERG = "pyiceberg_"
CATALOG = "catalog"
HOME = "HOME"
PYICEBERG_HOME = "PYICEBERG_HOME"
PYICEBERG_YML = ".pyiceberg.yaml"

logger = logging.getLogger(__name__)


def merge_config(lhs: RecursiveDict, rhs: RecursiveDict) -> RecursiveDict:
    """merges right-hand side into the left-hand side"""
    new_config = lhs.copy()
    for rhs_key, rhs_value in rhs.items():
        if rhs_key in new_config:
            lhs_value = new_config[rhs_key]
            if isinstance(lhs_value, dict) and isinstance(rhs_value, dict):
                # If they are both dicts, then we have to go deeper
                new_config[rhs_key] = merge_config(lhs_value, rhs_value)
            else:
                # Take the non-null value, with precedence on rhs
                new_config[rhs_key] = lhs_value or rhs_value
        else:
            # New key
            new_config[rhs_key] = rhs_value

    return new_config


def _lowercase_dictionary_keys(input_dict: RecursiveDict) -> RecursiveDict:
    """Lowers all the keys of a dictionary in a recursive manner, to make the lookup case-insensitive"""
    return {k.lower(): _lowercase_dictionary_keys(v) if isinstance(v, dict) else v for k, v in input_dict.items()}


class Config:
    config: RecursiveDict

    def __init__(self):
        config = self._from_configuration_files() or {}
        config = merge_config(config, self._from_environment_variables(config))
        self.config = FrozenDict(**config)

    @staticmethod
    def _from_configuration_files() -> Optional[RecursiveDict]:
        """Loads the first configuration file that its finds

        Will first look in the PYICEBERG_HOME env variable,
        and then in the home directory.
        """

        def _load_yaml(directory: Optional[str]) -> Optional[RecursiveDict]:
            if directory:
                path = f"{directory.rstrip('/')}/{PYICEBERG_YML}"
                if os.path.isfile(path):
                    with open(path, encoding="utf-8") as f:
                        file_config = yaml.safe_load(f)
                        file_config_lowercase = _lowercase_dictionary_keys(file_config)
                        return file_config_lowercase
            return None

        # Give priority to the PYICEBERG_HOME directory
        if pyiceberg_home_config := _load_yaml(os.environ.get(PYICEBERG_HOME)):
            return pyiceberg_home_config
        # Look into the home directory
        if pyiceberg_home_config := _load_yaml(os.environ.get(HOME)):
            return pyiceberg_home_config
        # Didn't find a config
        return None

    @staticmethod
    def _from_environment_variables(config: RecursiveDict) -> RecursiveDict:
        """Reads the environment variables, to check if there are any prepended by PYICEBERG_

        Args:
            config: Existing configuration that's being amended with configuration from environment variables

        Returns:
            Amended configuration
        """

        def set_property(_config: RecursiveDict, path: List[str], config_value: str):
            while len(path) > 0:
                element = path.pop(0)
                if len(path) == 0:
                    # We're at the end
                    _config[element] = config_value
                else:
                    # We have to go deeper
                    if element not in _config:
                        _config[element] = {}
                    if isinstance(_config[element], dict):
                        _config = _config[element]  # type: ignore
                    else:
                        raise ValueError(
                            f"Incompatible configurations, merging dict with a value: {'.'.join(path)}, value: {config_value}"
                        )

        for env_var, config_value in os.environ.items():
            # Make it lowercase to make it case-insensitive
            env_var_lower = env_var.lower()
            if env_var_lower.startswith(PYICEBERG.lower()):
                key = env_var_lower[len(PYICEBERG) :]
                parts = key.split("__")
                parts_normalized = [part.replace("_", "-") for part in parts]
                set_property(config, parts_normalized, config_value)

        return config

    def get_catalog_config(self, catalog_name: str) -> Optional[RecursiveDict]:
        if CATALOG in self.config:
            catalog_name_lower = catalog_name.lower()
            catalogs = self.config[CATALOG]
            if not isinstance(catalogs, dict):
                raise ValueError(f"Catalog configurations needs to be an object: {catalog_name}")
            if catalog_name_lower in catalogs:
                catalog_conf = catalogs[catalog_name_lower]
                assert isinstance(catalog_conf, dict), f"Configuration path catalogs.{catalog_name_lower} needs to be an object"
                return catalog_conf
        return None
