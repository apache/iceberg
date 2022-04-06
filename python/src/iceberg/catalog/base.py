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

from typing import Tuple

import mmh3  # type: ignore


class Namespace:
    """A namespace in a Catalog

    Example:
        >>> from iceberg.catalog.base import Namespace
        >>> namespace = Namespace("foo", "bar", "baz")
        >>> print(namespace)
        foo.bar.baz
    """

    def __init__(self, *levels: str):
        self._levels = levels
        for level in levels:
            if not level:
                raise ValueError("Cannot create a namespace with a None level")
            if b"\x00" in bytes(level.encode("utf-8")):
                raise ValueError(f"Cannot create a namespace with the null-byte character: {level}")

    def __str__(self) -> str:
        """Each level in the namespace is combined into a dot-separatd string"""
        return ".".join(self.levels)

    def __repr__(self) -> str:
        levels = ", ".join((f'"{level}"' for level in self.levels))  # Join levels into a quoted and comma-separated string
        return f"Namespace({levels})"

    def __len__(self) -> int:
        """The length of a Namespace is determined by the number of levels"""
        return len(self.levels)

    def __eq__(self, other) -> bool:
        if id(self) == id(other):
            return True
        if not isinstance(other, Namespace):
            return False
        return self.levels == other.levels

    def __hash__(self) -> int:
        return mmh3.hash("".join(self.levels))

    @property
    def levels(self) -> Tuple[str, ...]:
        """Returns the levels for the namespace as a tuple"""
        return self._levels

    def level(self, pos: int) -> str:
        """Returns a single level at a specific position

        Args:
            pos (int): An index position of a level

        Raises:
            ValueError: If no level exists at position `pos`

        Returns:
            str: The level at position `pos` in the namespace
        """
        try:
            level_at_pos = self.levels[pos]
        except IndexError:
            raise ValueError(f"Cannot find level at position: {pos}")
        return level_at_pos

    def empty(self) -> bool:
        """Returns False is the namespace has no levels"""
        return len(self.levels) == 0
