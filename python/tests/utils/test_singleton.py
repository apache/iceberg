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
from pyiceberg.avro.reader import BooleanReader, FixedReader
from pyiceberg.transforms import VoidTransform


def test_singleton() -> None:
    """We want to reuse the readers to avoid creating a gazillion of them"""
    assert id(BooleanReader()) == id(BooleanReader())
    assert id(FixedReader(22)) == id(FixedReader(22))
    assert id(FixedReader(19)) != id(FixedReader(25))


def test_singleton_transform() -> None:
    """We want to reuse VoidTransform since it doesn't carry any state"""
    assert id(VoidTransform()) == id(VoidTransform())
