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
"""Concurrency concepts that support efficient multi-threading."""
from concurrent.futures import Executor, ThreadPoolExecutor
from typing import Optional

from pyiceberg.exceptions import InvalidConfigurationError
from pyiceberg.utils.config import Config


class ExecutorFactory:
    __instance: Optional[Executor] = None

    @staticmethod
    def create() -> Executor:
        """Returns the same executor in each call."""
        if ExecutorFactory.__instance is None:
            max_workers = ExecutorFactory.max_workers()
            ExecutorFactory.__instance = ThreadPoolExecutor(max_workers=max_workers)

        return ExecutorFactory.__instance

    @staticmethod
    def max_workers() -> Optional[int]:
        """Returns the max number of workers configured."""
        config = Config()
        val = config.config.get("max-workers")

        if val is None:
            return None

        try:
            return int(val)  # type: ignore
        except ValueError as err:
            raise InvalidConfigurationError(f"Max workers should be an integer or left unset. Current value: {val}") from err
