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

from decimal import Decimal, getcontext

from iceberg.api.expressions import Literal
from iceberg.api.transforms import (Bucket,
                                    BucketDouble,
                                    BucketFloat)
from iceberg.api.types import (DateType,
                               DecimalType,
                               IntegerType,
                               LongType,
                               TimestampType,
                               TimeType)
import pytest


@pytest.mark.parametrize("test_input,test_type,expected", [
    (1, IntegerType, 1392991556),
    (34, IntegerType, 2017239379),
    (34, LongType, 2017239379)])
def test_spec_values_int(test_input, test_type, expected):
    assert Bucket.get(test_type.get(), 100).hash(test_input) == expected


@pytest.mark.parametrize("test_input,test_type,expected", [
    (1, BucketFloat(100), -142385009),
    (1, BucketDouble(100), -142385009)])
def test_spec_values_dbl(test_input, test_type, expected):
    assert test_type.hash(test_input) == expected


@pytest.mark.parametrize("test_input,test_type,scale_factor,expected", [
    (Decimal("14.20"), DecimalType.of(9, 2), Decimal(10) ** -2, -500754589),
    (Decimal("137302769811943318102518958871258.37580"), DecimalType.of(38, 5), Decimal(10) ** -5, -32334285)])
def test_spec_values_dec(test_input, test_type, scale_factor, expected):
    getcontext().prec = 38
    assert Bucket.get(test_type, 100).hash(test_input.quantize(scale_factor)) == expected


@pytest.mark.parametrize("test_input,test_type,expected", [
    (Literal.of("2017-11-16").to(DateType.get()), DateType.get(), -653330422),
    (Literal.of("22:31:08").to(TimeType.get()), TimeType.get(), -662762989),
    (Literal.of("2017-11-16T22:31:08").to(TimestampType.without_timezone()),
     TimestampType.without_timezone(), -2047944441),
    (Literal.of("2017-11-16T14:31:08-08:00").to(TimestampType.with_timezone()),
     TimestampType.with_timezone(), -2047944441)])
def test_spec_values_datetime_uuid(test_input, test_type, expected):
    assert Bucket.get(test_type, 100).hash(test_input.value) == expected
