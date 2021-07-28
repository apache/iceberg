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
import decimal
import unittest
import uuid

from iceberg.api.transforms import Transforms
from iceberg.api.types import (BinaryType,
                               DateType,
                               DecimalType,
                               FixedType,
                               IntegerType,
                               LongType,
                               StringType,
                               TimestampType,
                               TimeType,
                               UUIDType)


class TestBucket(unittest.TestCase):

    def test_bucket_hash(self):
        buckets = [
            [Transforms.bucket(IntegerType.get(), 100), 34, 2017239379],
            [Transforms.bucket(LongType.get(), 100), 34, 2017239379],
            [Transforms.bucket(DateType.get(), 100), 17486, -653330422],
            [Transforms.bucket(TimeType.get(), 100), 81068000000, -662762989],
            [Transforms.bucket(TimestampType.without_timezone(), 100), 1510871468000000, -2047944441],
            [Transforms.bucket(DecimalType.of(9, 2), 100), decimal.Decimal("14.20"), -500754589],
            [Transforms.bucket(StringType.get(), 100), "iceberg", 1210000089],
            [Transforms.bucket(UUIDType.get(), 100), uuid.UUID("f79c3e09-677c-4bbd-a479-3f349cb785e7"), 1488055340],
            [Transforms.bucket(FixedType.of_length(3), 128), b'foo', -156908512],
            [Transforms.bucket(BinaryType.get(), 128), b'\x00\x01\x02\x03', -188683207]
        ]

        for bucket in buckets:
            self.assertEqual(bucket[2], bucket[0].hash(bucket[1]))
