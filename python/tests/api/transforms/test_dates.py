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


from iceberg.api.expressions import Literal
from iceberg.api.transforms import Transforms
from iceberg.api.types import (DateType, IntegerType)
import pytest


@pytest.mark.parametrize("date,expected_year, expected_month, expected_day", [
    (Literal.of("2017-12-01").to(DateType.get()), 47, 575, 17501),
    (Literal.of("1970-01-01").to(DateType.get()), 0, 0, 0),
    (Literal.of("1969-12-31").to(DateType.get()), -1, -1, -1)])
def test_date_transform(date, expected_year, expected_month, expected_day):
    assert Transforms.year(DateType.get()).apply(date.value) == expected_year
    assert Transforms.month(DateType.get()).apply(date.value) == expected_month
    assert Transforms.day(DateType.get()).apply(date.value) == expected_day


@pytest.mark.parametrize("transform_gran,expected", [
    (Transforms.year, "2017"),
    (Transforms.month, "2017-12"),
    (Transforms.day, "2017-12-01")])
def test_date_to_human_string(transform_gran, expected):
    type_var = DateType.get()
    date = Literal.of("2017-12-01").to(type_var)

    assert (transform_gran(DateType.get())
            .to_human_string(transform_gran(DateType.get())
                             .apply(date.value))) == expected


@pytest.mark.parametrize("transform_gran,expected", [
    (Transforms.year, "1969"),
    (Transforms.month, "1969-12"),
    (Transforms.day, "1969-12-30")
])
def test_negative_date_to_human_string(transform_gran, expected):
    type_var = DateType.get()
    date = Literal.of("1969-12-30").to(type_var)

    transform = transform_gran(type_var)
    assert transform.to_human_string(transform.apply(date.value)) == expected


@pytest.mark.parametrize("transform_gran,expected", [
    (Transforms.year, "1970"),
    (Transforms.month, "1970-01"),
    (Transforms.day, "1970-01-01")
])
def test_date_to_human_string_lower_bound(transform_gran, expected):
    type_var = DateType.get()
    date = Literal.of("1970-01-01").to(type_var)

    transform = transform_gran(type_var)
    assert transform.to_human_string(transform.apply(date.value)) == expected


@pytest.mark.parametrize("transform_gran,expected", [
    (Transforms.year, "1969"),
    (Transforms.month, "1969-01"),
    (Transforms.day, "1969-01-01")
])
def test_nagative_date_to_human_string_lower_bound(transform_gran, expected):
    type_var = DateType.get()
    date = Literal.of("1969-01-01").to(type_var)

    transform = transform_gran(type_var)
    assert transform.to_human_string(transform.apply(date.value)) == expected


@pytest.mark.parametrize("transform_gran,expected", [
    (Transforms.year, "1969"),
    (Transforms.month, "1969-12"),
    (Transforms.day, "1969-12-31")
])
def test_nagative_date_to_human_string_upper_bound(transform_gran, expected):
    type_var = DateType.get()
    date = Literal.of("1969-12-31").to(type_var)

    transform = transform_gran(type_var)
    assert transform.to_human_string(transform.apply(date.value)) == expected


@pytest.mark.parametrize("transform_gran", [
    Transforms.year,
    Transforms.month,
    Transforms.day])
def test_null_human_string(transform_gran):
    type_var = DateType.get()
    assert transform_gran(type_var).to_human_string(None) == "null"


@pytest.mark.parametrize("transform_gran,expected", [
    (Transforms.year, IntegerType.get()),
    (Transforms.month, IntegerType.get()),
    (Transforms.day, DateType.get())
])
def test_dates_return_type(transform_gran, expected):
    type_var = DateType.get()

    transform = transform_gran(type_var)
    result_type = transform.get_result_type(type_var)
    assert result_type == expected
