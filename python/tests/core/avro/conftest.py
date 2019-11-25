# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from tempfile import NamedTemporaryFile

from iceberg.api import Schema
from iceberg.api.types import (BinaryType,
                               BooleanType,
                               DateType,
                               DecimalType,
                               DoubleType,
                               FixedType,
                               FloatType,
                               IntegerType,
                               LongType,
                               NestedField,
                               StringType,
                               StructType,
                               TimestampType,
                               UUIDType)
import pytest


@pytest.fixture(scope="session")
def supported_primitives():
    return StructType.of([NestedField.required(100, "id", LongType.get()),
                          NestedField.optional(101, "data", StringType.get()),
                          NestedField.required(102, "b", BooleanType.get()),
                          NestedField.optional(103, "i", IntegerType.get()),
                          NestedField.required(104, "l", LongType.get()),
                          NestedField.optional(105, "f", FloatType.get()),
                          NestedField.required(106, "d", DoubleType.get()),
                          NestedField.optional(107, "date", DateType.get()),
                          NestedField.required(108, "ts", TimestampType.with_timezone()),
                          NestedField.required(110, "s", StringType.get()),
                          NestedField.required(111, "uuid", UUIDType.get()),
                          NestedField.required(112, "fixed", FixedType.of_length(7)),
                          NestedField.optional(113, "bytes", BinaryType.get()),
                          NestedField.required(114, "dec_9_0", DecimalType.of(9, 0)),
                          NestedField.required(114, "dec_11_2", DecimalType.of(11, 2)),
                          NestedField.required(114, "dec_38_10", DecimalType.of(38, 10))])


@pytest.fixture(scope="session")
def iceberg_full_read_projection_schema():
    return Schema([NestedField.required(0, "id", LongType.get()),
                   NestedField.optional(1, "data", StringType.get())])


def write_and_read(desc, write_schema, read_schema, record):
    with NamedTemporaryFile(delete=True, mode='wb') as temp_file:
        return temp_file
