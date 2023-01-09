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
import pytest

from pyiceberg.typedef import FrozenDict, KeyDefaultDict, Record
from pyiceberg.types import IntegerType, NestedField, StringType


def test_setitem_frozendict() -> None:
    d = FrozenDict(foo=1, bar=2)
    with pytest.raises(AttributeError):
        d["foo"] = 3


def test_update_frozendict() -> None:
    d = FrozenDict(foo=1, bar=2)
    with pytest.raises(AttributeError):
        d.update({"yes": 2})


def test_keydefaultdict() -> None:
    def one(_: int) -> int:
        return 1

    defaultdict = KeyDefaultDict(one)
    assert defaultdict[22] == 1


def test_record_repr() -> None:
    r = Record.of(1, "vo", True)
    assert repr(r) == "Record[1, 'vo', True]"


def test_named_record() -> None:
    r = Record(fields=(NestedField(0, "id", IntegerType()), NestedField(1, "name", StringType())))

    r[0] = 123
    r[1] = "abc"

    assert r[0] == 123
    assert r[1] == "abc"
