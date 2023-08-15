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

from pyiceberg.utils.lazydict import LazyDict


def test_lazy_dict_ints() -> None:
    lazy_dict = LazyDict[int, int]([[1, 2], [3, 4]])
    assert lazy_dict[1] == 2
    assert lazy_dict[3] == 4


def test_lazy_dict_strings() -> None:
    lazy_dict = LazyDict[int, str]([[1, "red", 5, "banana"], [3, "blue"]])
    assert lazy_dict[1] == "red"
    assert lazy_dict[3] == "blue"
    assert lazy_dict[5] == "banana"
