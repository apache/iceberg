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

from iceberg.partition_field import PartitionField


def test_partition_field_equality():
    part_field_1 = PartitionField(1, 2, "test_1")
    part_field_2 = PartitionField(1, 2, "test_1")
    assert part_field_1 == part_field_2


def test_partition_field_str():
    part_field_1 = PartitionField(1, 2, "test_1")
    assert str(part_field_1) == "2: test_1 (1)"
