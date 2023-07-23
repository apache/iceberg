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

from typing import (
    Dict,
    Iterator,
    Mapping,
    Tuple,
)


class LazyDictIntInt(Mapping[int, int]):
    """Lazily build a dictionary from an array of integers."""

    __slots__ = ("_contents", "_dict", "_did_build", "_len")
    _dict: Dict[int, int]

    def __init__(self, contents: Tuple[Tuple[int, ...], ...]):
        self._contents = contents
        self._did_build = False
        self._len = len(self._contents) // 2

    def _build_dict(self) -> None:
        if not self._did_build:
            self._did_build = True
            self._dict = {}
            for item in self._contents:
                self._dict.update(dict(zip(item[::2], item[1::2])))

    def __getitem__(self, key: int, /) -> int:
        """Returns the value for the given key."""
        self._build_dict()
        return self._dict[key]

    def __iter__(self) -> Iterator[int]:
        """Returns an iterator over the keys of the dictionary."""
        self._build_dict()
        return iter(self._dict)

    def __len__(self) -> int:
        """Returns the number of items in the dictionary."""
        return self._len
