#
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
#

from iceberg.api import PartitionSpec, PartitionSpecBuilder, Schema
from iceberg.api.types import IntegerType, NestedField, StringType
import pytest


@pytest.fixture(scope="session")
def base_scan_schema():
    return Schema([NestedField.required(1, "id", IntegerType.get()),
                   NestedField.required(2, "data", StringType.get())])


@pytest.fixture(scope="session", params=["none", "one"])
def base_scan_partition(base_scan_schema, request):
    if request.param == "none":
        spec = PartitionSpec.unpartitioned()
    else:
        spec = PartitionSpecBuilder(base_scan_schema).add(1, 10000, "id", "identity").build()
    return spec
