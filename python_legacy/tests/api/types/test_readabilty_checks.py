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

import unittest

from iceberg.api.types import (BinaryType,
                               BooleanType,
                               DateType,
                               DecimalType,
                               DoubleType,
                               FixedType,
                               FloatType,
                               IntegerType,
                               LongType,
                               StringType,
                               TimestampType,
                               TimeType,
                               UUIDType)

PRIMITIVES = [BinaryType.get(),
              BooleanType.get(),
              DateType.get(),
              DecimalType.of(9, 2),
              DecimalType.of(11, 2),
              DecimalType.of(9, 3),
              DoubleType.get(),
              FixedType.of_length(3),
              FixedType.of_length(4),
              FloatType.get(),
              IntegerType.get(),
              LongType.get(),
              StringType.get(),
              TimestampType.with_timezone(),
              TimestampType.without_timezone(),
              TimeType.get(),
              UUIDType.get()]


class TestReadabilityChecks(unittest.TestCase):

    def test_primitive_types(self):
        # TO-DO:  Need to implement CheckCompatibility in type_util
        pass
