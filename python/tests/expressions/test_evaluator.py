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
# pylint:disable=redefined-outer-name
from typing import Any

import pytest

from pyiceberg.conversions import to_bytes
from pyiceberg.expressions import (
    And,
    EqualTo,
    GreaterThan,
    GreaterThanOrEqual,
    In,
    IsNaN,
    IsNull,
    LessThan,
    LessThanOrEqual,
    Not,
    NotEqualTo,
    NotIn,
    NotNaN,
    NotNull,
    NotStartsWith,
    Or,
    StartsWith,
)
from pyiceberg.expressions.visitors import _InclusiveMetricsEvaluator
from pyiceberg.manifest import DataFile, FileFormat
from pyiceberg.schema import Schema
from pyiceberg.types import (
    DoubleType,
    FloatType,
    IcebergType,
    IntegerType,
    NestedField,
    PrimitiveType,
    StringType,
)

INT_MIN_VALUE = 30
INT_MAX_VALUE = 79


def _to_byte_buffer(field_type: IcebergType, val: Any) -> bytes:
    if not isinstance(field_type, PrimitiveType):
        raise ValueError(f"Expected a PrimitiveType, got: {type(field_type)}")
    return to_bytes(field_type, val)


INT_MIN = _to_byte_buffer(IntegerType(), INT_MIN_VALUE)
INT_MAX = _to_byte_buffer(IntegerType(), INT_MAX_VALUE)

STRING_MIN = _to_byte_buffer(StringType(), "a")
STRING_MAX = _to_byte_buffer(StringType(), "z")


@pytest.fixture
def schema_data_file() -> Schema:
    return Schema(
        NestedField(1, "id", IntegerType(), required=True),
        NestedField(2, "no_stats", IntegerType(), required=False),
        NestedField(3, "required", StringType(), required=True),
        NestedField(4, "all_nulls", StringType(), required=False),
        NestedField(5, "some_nulls", StringType(), required=False),
        NestedField(6, "no_nulls", StringType(), required=False),
        NestedField(7, "all_nans", DoubleType(), required=False),
        NestedField(8, "some_nans", FloatType(), required=False),
        NestedField(9, "no_nans", FloatType(), required=False),
        NestedField(10, "all_nulls_double", DoubleType(), required=False),
        NestedField(11, "all_nans_v1_stats", FloatType(), required=False),
        NestedField(12, "nan_and_null_only", DoubleType(), required=False),
        NestedField(13, "no_nan_stats", DoubleType(), required=False),
        NestedField(14, "some_empty", StringType(), required=False),
    )


@pytest.fixture
def data_file() -> DataFile:
    return DataFile(
        file_path="file_1.parquet",
        file_format=FileFormat.PARQUET,
        partition={},
        record_count=50,
        file_size_in_bytes=3,
        value_counts={
            4: 50,
            5: 50,
            6: 50,
            7: 50,
            8: 50,
            9: 50,
            10: 50,
            11: 50,
            12: 50,
            13: 50,
            14: 50,
        },
        null_value_counts={4: 50, 5: 10, 6: 0, 10: 50, 11: 0, 12: 1, 14: 8},
        nan_value_counts={
            7: 50,
            8: 10,
            9: 0,
        },
        lower_bounds={
            1: to_bytes(IntegerType(), INT_MIN_VALUE),
            11: to_bytes(FloatType(), float("nan")),
            12: to_bytes(DoubleType(), float("nan")),
            14: to_bytes(StringType(), ""),
        },
        upper_bounds={
            1: to_bytes(IntegerType(), INT_MAX_VALUE),
            11: to_bytes(FloatType(), float("nan")),
            12: to_bytes(DoubleType(), float("nan")),
            14: to_bytes(StringType(), "房东整租霍营小区二层两居室"),
        },
    )


@pytest.fixture
def data_file_2() -> DataFile:
    return DataFile(
        file_path="file_2.parquet",
        file_format=FileFormat.PARQUET,
        partition={},
        record_count=50,
        file_size_in_bytes=3,
        value_counts={3: 20},
        null_value_counts={3: 2},
        nan_value_counts=None,
        lower_bounds={3: to_bytes(StringType(), "aa")},
        upper_bounds={3: to_bytes(StringType(), "dC")},
    )


@pytest.fixture
def data_file_3() -> DataFile:
    return DataFile(
        file_path="file_3.parquet",
        file_format=FileFormat.PARQUET,
        partition={},
        record_count=50,
        file_size_in_bytes=3,
        value_counts={3: 20},
        null_value_counts={3: 2},
        nan_value_counts=None,
        lower_bounds={3: to_bytes(StringType(), "1str1")},
        upper_bounds={3: to_bytes(StringType(), "3str3")},
    )


@pytest.fixture
def data_file_4() -> DataFile:
    return DataFile(
        file_path="file_4.parquet",
        file_format=FileFormat.PARQUET,
        partition={},
        record_count=50,
        file_size_in_bytes=3,
        value_counts={3: 20},
        null_value_counts={3: 2},
        nan_value_counts=None,
        lower_bounds={3: to_bytes(StringType(), "abc")},
        upper_bounds={3: to_bytes(StringType(), "イロハニホヘト")},
    )


def test_all_null(schema_data_file: Schema, data_file: DataFile) -> None:
    should_read = _InclusiveMetricsEvaluator(schema_data_file, NotNull("all_nulls")).eval(data_file)
    assert not should_read, "Should skip: no non-null value in all null column"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, LessThan("all_nulls", "a")).eval(data_file)
    assert not should_read, "Should skip: lessThan on all null column"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, LessThanOrEqual("all_nulls", "a")).eval(data_file)
    assert not should_read, "Should skip: lessThanOrEqual on all null column"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, GreaterThan("all_nulls", "a")).eval(data_file)
    assert not should_read, "Should skip: greaterThan on all null column"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, GreaterThanOrEqual("all_nulls", "a")).eval(data_file)
    assert not should_read, "Should skip: greaterThanOrEqual on all null column"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, EqualTo("all_nulls", "a")).eval(data_file)
    assert not should_read, "Should skip: equal on all null column"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, NotNull("some_nulls")).eval(data_file)
    assert should_read, "Should read: column with some nulls contains a non-null value"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, NotNull("no_nulls")).eval(data_file)
    assert should_read, "Should read: non-null column contains a non-null value"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, StartsWith("all_nulls", "asad")).eval(data_file)
    assert not should_read, "Should skip: startsWith on all null column"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, NotStartsWith("all_nulls", "asad")).eval(data_file)
    assert should_read, "Should read: notStartsWith on all null column"


def test_no_nulls(schema_data_file: Schema, data_file: DataFile) -> None:
    should_read = _InclusiveMetricsEvaluator(schema_data_file, IsNull("all_nulls")).eval(data_file)
    assert should_read, "Should read: at least one null value in all null column"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, IsNull("some_nulls")).eval(data_file)
    assert should_read, "Should read: column with some nulls contains a null value"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, IsNull("no_nulls")).eval(data_file)
    assert not should_read, "Should skip: non-null column contains no null values"


def test_is_nan(schema_data_file: Schema, data_file: DataFile) -> None:
    should_read = _InclusiveMetricsEvaluator(schema_data_file, IsNaN("all_nans")).eval(data_file)
    assert should_read, "Should read: at least one nan value in all nan column"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, IsNaN("some_nans")).eval(data_file)
    assert should_read, "Should read: at least one nan value in some nan column"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, IsNaN("no_nans")).eval(data_file)
    assert not should_read, "Should skip: no-nans column contains no nan values"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, IsNaN("all_nulls_double")).eval(data_file)
    assert not should_read, "Should skip: all-null column doesn't contain nan value"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, IsNaN("no_nan_stats")).eval(data_file)
    assert should_read, "Should read: no guarantee on if contains nan value without nan stats"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, IsNaN("all_nans_v1_stats")).eval(data_file)
    assert should_read, "Should read: at least one nan value in all nan column"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, IsNaN("nan_and_null_only")).eval(data_file)
    assert should_read, "Should read: at least one nan value in nan and nulls only column"


def test_not_nan(schema_data_file: Schema, data_file: DataFile) -> None:
    should_read = _InclusiveMetricsEvaluator(schema_data_file, NotNaN("all_nans")).eval(data_file)
    assert not should_read, "Should skip: column with all nans will not contain non-nan"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, NotNaN("some_nans")).eval(data_file)
    assert should_read, "Should read: at least one non-nan value in some nan column"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, NotNaN("no_nans")).eval(data_file)
    assert should_read, "Should read: at least one non-nan value in no nan column"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, NotNaN("all_nulls_double")).eval(data_file)
    assert should_read, "Should read: at least one non-nan value in all null column"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, NotNaN("no_nan_stats")).eval(data_file)
    assert should_read, "Should read: no guarantee on if contains nan value without nan stats"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, NotNaN("all_nans_v1_stats")).eval(data_file)
    assert should_read, "Should read: no guarantee on if contains nan value without nan stats"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, NotNaN("nan_and_null_only")).eval(data_file)
    assert should_read, "Should read: at least one null value in nan and nulls only column"


def test_required_column(schema_data_file: Schema, data_file: DataFile) -> None:
    should_read = _InclusiveMetricsEvaluator(schema_data_file, NotNull("required")).eval(data_file)
    assert should_read, "Should read: required columns are always non-null"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, IsNull("required")).eval(data_file)
    assert not should_read, "Should skip: required columns are always non-null"


def test_missing_column(schema_data_file: Schema, data_file: DataFile) -> None:
    with pytest.raises(ValueError) as exc_info:
        _ = _InclusiveMetricsEvaluator(schema_data_file, LessThan("missing", 22)).eval(data_file)

    assert str(exc_info.value) == "Could not find field with name missing, case_sensitive=True"


def test_missing_stats() -> None:
    no_stats_schema = Schema(
        NestedField(2, "no_stats", DoubleType(), required=False),
    )

    no_stats_file = DataFile(
        file_path="file_1.parquet",
        file_format=FileFormat.PARQUET,
        partition={},
        record_count=50,
        value_counts=None,
        null_value_counts=None,
        nan_value_counts=None,
        lower_bounds=None,
        upper_bounds=None,
    )

    expressions = [
        LessThan("no_stats", 5),
        LessThanOrEqual("no_stats", 30),
        EqualTo("no_stats", 70),
        GreaterThan("no_stats", 78),
        GreaterThanOrEqual("no_stats", 90),
        NotEqualTo("no_stats", 101),
        IsNull("no_stats"),
        NotNull("no_stats"),
        IsNaN("no_stats"),
        NotNaN("no_stats"),
    ]

    for expression in expressions:
        should_read = _InclusiveMetricsEvaluator(no_stats_schema, expression).eval(no_stats_file)
        assert should_read, f"Should read when stats are missing for: {expression}"


def test_zero_record_file_stats(schema_data_file: Schema) -> None:
    zero_record_data_file = DataFile(file_path="file_1.parquet", file_format=FileFormat.PARQUET, partition={}, record_count=0)

    expressions = [
        LessThan("no_stats", 5),
        LessThanOrEqual("no_stats", 30),
        EqualTo("no_stats", 70),
        GreaterThan("no_stats", 78),
        GreaterThanOrEqual("no_stats", 90),
        NotEqualTo("no_stats", 101),
        IsNull("no_stats"),
        NotNull("no_stats"),
        IsNaN("no_stats"),
        NotNaN("no_stats"),
    ]

    for expression in expressions:
        should_read = _InclusiveMetricsEvaluator(schema_data_file, expression).eval(zero_record_data_file)
        assert not should_read, f"Should skip a datafile without records: {expression}"


def test_not(schema_data_file: Schema, data_file: DataFile) -> None:
    should_read = _InclusiveMetricsEvaluator(schema_data_file, Not(LessThan("id", INT_MIN_VALUE - 25))).eval(data_file)
    assert should_read, "Should read: not(false)"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, Not(GreaterThan("id", INT_MIN_VALUE - 25))).eval(data_file)
    assert not should_read, "Should skip: not(true)"


def test_and(schema_data_file: Schema, data_file: DataFile) -> None:
    should_read = _InclusiveMetricsEvaluator(
        schema_data_file, And(LessThan("id", INT_MIN_VALUE - 25), GreaterThanOrEqual("id", INT_MIN_VALUE - 30))
    ).eval(data_file)
    assert not should_read, "Should skip: and(false, true)"

    should_read = _InclusiveMetricsEvaluator(
        schema_data_file, And(LessThan("id", INT_MIN_VALUE - 25), GreaterThanOrEqual("id", INT_MIN_VALUE + 1))
    ).eval(data_file)
    assert not should_read, "Should skip: and(false, false)"

    should_read = _InclusiveMetricsEvaluator(
        schema_data_file, And(GreaterThan("id", INT_MIN_VALUE - 25), LessThanOrEqual("id", INT_MIN_VALUE))
    ).eval(data_file)
    assert should_read, "Should read: and(true, true)"


def test_or(schema_data_file: Schema, data_file: DataFile) -> None:
    should_read = _InclusiveMetricsEvaluator(
        schema_data_file, Or(LessThan("id", INT_MIN_VALUE - 25), GreaterThanOrEqual("id", INT_MAX_VALUE + 1))
    ).eval(data_file)
    assert not should_read, "Should skip: or(false, false)"

    should_read = _InclusiveMetricsEvaluator(
        schema_data_file, Or(LessThan("id", INT_MIN_VALUE - 25), GreaterThanOrEqual("id", INT_MAX_VALUE - 19))
    ).eval(data_file)
    assert should_read, "Should read: or(false, true)"


def test_integer_lt(schema_data_file: Schema, data_file: DataFile) -> None:
    should_read = _InclusiveMetricsEvaluator(schema_data_file, LessThan("id", INT_MIN_VALUE - 25)).eval(data_file)
    assert not should_read, "Should not read: id range below lower bound (5 < 30)"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, LessThan("id", INT_MIN_VALUE)).eval(data_file)
    assert not should_read, "Should not read: id range below lower bound (30 is not < 30)"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, LessThan("id", INT_MIN_VALUE + 1)).eval(data_file)
    assert should_read, "Should read: one possible id"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, LessThan("id", INT_MAX_VALUE)).eval(data_file)
    assert should_read, "Should read: may possible ids"


def test_integer_lt_eq(schema_data_file: Schema, data_file: DataFile) -> None:
    should_read = _InclusiveMetricsEvaluator(schema_data_file, LessThanOrEqual("id", INT_MIN_VALUE - 25)).eval(data_file)
    assert not should_read, "Should not read: id range below lower bound (5 < 30)"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, LessThanOrEqual("id", INT_MIN_VALUE - 1)).eval(data_file)
    assert not should_read, "Should not read: id range below lower bound (30 is not < 30)"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, LessThanOrEqual("id", INT_MIN_VALUE)).eval(data_file)
    assert should_read, "Should read: one possible id"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, LessThanOrEqual("id", INT_MAX_VALUE)).eval(data_file)
    assert should_read, "Should read: may possible ids"


def test_integer_gt(schema_data_file: Schema, data_file: DataFile) -> None:
    should_read = _InclusiveMetricsEvaluator(schema_data_file, GreaterThan("id", INT_MAX_VALUE + 6)).eval(data_file)
    assert not should_read, "Should not read: id range above upper bound (85 < 79)"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, GreaterThan("id", INT_MAX_VALUE)).eval(data_file)
    assert not should_read, "Should not read: id range above upper bound (79 is not > 79)"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, GreaterThan("id", INT_MIN_VALUE - 1)).eval(data_file)
    assert should_read, "Should read: one possible id"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, GreaterThan("id", INT_MAX_VALUE - 4)).eval(data_file)
    assert should_read, "Should read: may possible ids"


def test_integer_gt_eq(schema_data_file: Schema, data_file: DataFile) -> None:
    should_read = _InclusiveMetricsEvaluator(schema_data_file, GreaterThanOrEqual("id", INT_MAX_VALUE + 6)).eval(data_file)
    assert not should_read, "Should not read: id range above upper bound (85 < 79)"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, GreaterThanOrEqual("id", INT_MAX_VALUE + 1)).eval(data_file)
    assert not should_read, "Should not read: id range above upper bound (80 > 79)"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, GreaterThanOrEqual("id", INT_MAX_VALUE)).eval(data_file)
    assert should_read, "Should read: one possible id"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, GreaterThanOrEqual("id", INT_MAX_VALUE - 4)).eval(data_file)
    assert should_read, "Should read: may possible ids"


def test_integer_eq(schema_data_file: Schema, data_file: DataFile) -> None:
    should_read = _InclusiveMetricsEvaluator(schema_data_file, EqualTo("id", INT_MIN_VALUE - 25)).eval(data_file)
    assert not should_read, "Should not read: id below lower bound"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, EqualTo("id", INT_MIN_VALUE - 1)).eval(data_file)
    assert not should_read, "Should not read: id below lower bound"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, EqualTo("id", INT_MIN_VALUE)).eval(data_file)
    assert should_read, "Should read: id equal to lower bound"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, EqualTo("id", INT_MAX_VALUE - 4)).eval(data_file)
    assert should_read, "Should read: id between lower and upper bounds"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, EqualTo("id", INT_MAX_VALUE)).eval(data_file)
    assert should_read, "Should read: id equal to upper bound"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, EqualTo("id", INT_MAX_VALUE + 1)).eval(data_file)
    assert not should_read, "Should not read: id above upper bound"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, EqualTo("id", INT_MAX_VALUE + 6)).eval(data_file)
    assert not should_read, "Should not read: id above upper bound"


def test_integer_not_eq(schema_data_file: Schema, data_file: DataFile) -> None:
    should_read = _InclusiveMetricsEvaluator(schema_data_file, NotEqualTo("id", INT_MIN_VALUE - 25)).eval(data_file)
    assert should_read, "Should read: id below lower bound"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, NotEqualTo("id", INT_MIN_VALUE - 1)).eval(data_file)
    assert should_read, "Should read: id below lower bound"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, NotEqualTo("id", INT_MIN_VALUE)).eval(data_file)
    assert should_read, "Should read: id equal to lower bound"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, NotEqualTo("id", INT_MAX_VALUE - 4)).eval(data_file)
    assert should_read, "Should read: id between lower and upper bounds"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, NotEqualTo("id", INT_MAX_VALUE)).eval(data_file)
    assert should_read, "Should read: id equal to upper bound"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, NotEqualTo("id", INT_MAX_VALUE + 1)).eval(data_file)
    assert should_read, "Should read: id above upper bound"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, NotEqualTo("id", INT_MAX_VALUE + 6)).eval(data_file)
    assert should_read, "Should read: id above upper bound"


def test_integer_not_eq_rewritten(schema_data_file: Schema, data_file: DataFile) -> None:
    should_read = _InclusiveMetricsEvaluator(schema_data_file, Not(EqualTo("id", INT_MIN_VALUE - 25))).eval(data_file)
    assert should_read, "Should read: id below lower bound"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, Not(EqualTo("id", INT_MIN_VALUE - 1))).eval(data_file)
    assert should_read, "Should read: id below lower bound"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, Not(EqualTo("id", INT_MIN_VALUE))).eval(data_file)
    assert should_read, "Should read: id equal to lower bound"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, Not(EqualTo("id", INT_MAX_VALUE - 4))).eval(data_file)
    assert should_read, "Should read: id between lower and upper bounds"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, Not(EqualTo("id", INT_MAX_VALUE))).eval(data_file)
    assert should_read, "Should read: id equal to upper bound"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, Not(EqualTo("id", INT_MAX_VALUE + 1))).eval(data_file)
    assert should_read, "Should read: id above upper bound"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, Not(EqualTo("id", INT_MAX_VALUE + 6))).eval(data_file)
    assert should_read, "Should read: id above upper bound"


def test_integer_case_insensitive_not_eq_rewritten(schema_data_file: Schema, data_file: DataFile) -> None:
    should_read = _InclusiveMetricsEvaluator(schema_data_file, Not(EqualTo("ID", INT_MIN_VALUE - 25)), case_sensitive=False).eval(
        data_file
    )
    assert should_read, "Should read: id below lower bound"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, Not(EqualTo("ID", INT_MIN_VALUE - 1)), case_sensitive=False).eval(
        data_file
    )
    assert should_read, "Should read: id below lower bound"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, Not(EqualTo("ID", INT_MIN_VALUE)), case_sensitive=False).eval(
        data_file
    )
    assert should_read, "Should read: id equal to lower bound"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, Not(EqualTo("ID", INT_MAX_VALUE - 4)), case_sensitive=False).eval(
        data_file
    )
    assert should_read, "Should read: id between lower and upper bounds"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, Not(EqualTo("ID", INT_MAX_VALUE)), case_sensitive=False).eval(
        data_file
    )
    assert should_read, "Should read: id equal to upper bound"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, Not(EqualTo("ID", INT_MAX_VALUE + 1)), case_sensitive=False).eval(
        data_file
    )
    assert should_read, "Should read: id above upper bound"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, Not(EqualTo("ID", INT_MAX_VALUE + 6)), case_sensitive=False).eval(
        data_file
    )
    assert should_read, "Should read: id above upper bound"


def test_missing_column_case_sensitive(schema_data_file: Schema, data_file: DataFile) -> None:
    with pytest.raises(ValueError) as exc_info:
        _ = _InclusiveMetricsEvaluator(schema_data_file, LessThan("ID", 22), case_sensitive=True).eval(data_file)

    assert str(exc_info.value) == "Could not find field with name ID, case_sensitive=True"


def test_integer_in(schema_data_file: Schema, data_file: DataFile) -> None:
    should_read = _InclusiveMetricsEvaluator(schema_data_file, In("id", {INT_MIN_VALUE - 25, INT_MIN_VALUE - 24})).eval(data_file)
    assert not should_read, "Should not read: id below lower bound (5 < 30, 6 < 30)"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, In("id", {INT_MIN_VALUE - 2, INT_MIN_VALUE - 1})).eval(data_file)
    assert not should_read, "Should not read: id below lower bound (28 < 30, 29 < 30)"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, In("id", {INT_MIN_VALUE - 1, INT_MIN_VALUE})).eval(data_file)
    assert should_read, "Should read: id equal to lower bound (30 == 30)"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, In("id", {INT_MAX_VALUE - 4, INT_MAX_VALUE - 3})).eval(data_file)
    assert should_read, "Should read: id between lower and upper bounds (30 < 75 < 79, 30 < 76 < 79)"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, In("id", {INT_MAX_VALUE, INT_MAX_VALUE + 1})).eval(data_file)
    assert should_read, "Should read: id equal to upper bound (79 == 79)"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, In("id", {INT_MAX_VALUE + 1, INT_MAX_VALUE + 2})).eval(data_file)
    assert not should_read, "Should not read: id above upper bound (80 > 79, 81 > 79)"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, In("id", {INT_MAX_VALUE + 6, INT_MAX_VALUE + 7})).eval(data_file)
    assert not should_read, "Should not read: id above upper bound (85 > 79, 86 > 79)"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, In("all_nulls", {"abc", "def"})).eval(data_file)
    assert not should_read, "Should skip: in on all nulls column"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, In("some_nulls", {"abc", "def"})).eval(data_file)
    assert should_read, "Should read: in on some nulls column"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, In("no_nulls", {"abc", "def"})).eval(data_file)
    assert should_read, "Should read: in on no nulls column"

    ids = list(range(400))
    should_read = _InclusiveMetricsEvaluator(schema_data_file, In("id", ids)).eval(data_file)
    assert should_read, "Should read: large in expression"


def test_integer_not_in(schema_data_file: Schema, data_file: DataFile) -> None:
    should_read = _InclusiveMetricsEvaluator(schema_data_file, NotIn("id", {INT_MIN_VALUE - 25, INT_MIN_VALUE - 24})).eval(
        data_file
    )
    assert should_read, "Should read: id below lower bound (5 < 30, 6 < 30)"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, NotIn("id", {INT_MIN_VALUE - 2, INT_MIN_VALUE - 1})).eval(
        data_file
    )
    assert should_read, "Should read: id below lower bound (28 < 30, 29 < 30)"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, NotIn("id", {INT_MIN_VALUE - 1, INT_MIN_VALUE})).eval(data_file)
    assert should_read, "Should read: id equal to lower bound (30 == 30)"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, NotIn("id", {INT_MAX_VALUE - 4, INT_MAX_VALUE - 3})).eval(
        data_file
    )
    assert should_read, "Should read: id between lower and upper bounds (30 < 75 < 79, 30 < 76 < 79)"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, NotIn("id", {INT_MAX_VALUE, INT_MAX_VALUE + 1})).eval(data_file)
    assert should_read, "Should read: id equal to upper bound (79 == 79)"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, NotIn("id", {INT_MAX_VALUE + 1, INT_MAX_VALUE + 2})).eval(
        data_file
    )
    assert should_read, "Should read: id above upper bound (80 > 79, 81 > 79)"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, NotIn("id", {INT_MAX_VALUE + 6, INT_MAX_VALUE + 7})).eval(
        data_file
    )
    assert should_read, "Should read: id above upper bound (85 > 79, 86 > 79)"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, NotIn("all_nulls", {"abc", "def"})).eval(data_file)
    assert should_read, "Should read: notIn on all nulls column"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, NotIn("some_nulls", {"abc", "def"})).eval(data_file)
    assert should_read, "Should read: in on some nulls column"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, NotIn("no_nulls", {"abc", "def"})).eval(data_file)
    assert should_read, "Should read: in on no nulls column"


@pytest.fixture
def schema_data_file_nan() -> Schema:
    return Schema(
        NestedField(1, "all_nan", DoubleType(), required=True),
        NestedField(2, "max_nan", DoubleType(), required=True),
        NestedField(3, "min_max_nan", FloatType(), required=False),
        NestedField(4, "all_nan_null_bounds", DoubleType(), required=True),
        NestedField(5, "some_nan_correct_bounds", FloatType(), required=False),
    )


@pytest.fixture
def data_file_nan() -> DataFile:
    return DataFile(
        file_path="file.avro",
        file_format=FileFormat.PARQUET,
        partition={},
        record_count=50,
        file_size_in_bytes=3,
        column_sizes={
            1: 10,
            2: 10,
            3: 10,
            4: 10,
            5: 10,
        },
        value_counts={
            1: 10,
            2: 10,
            3: 10,
            4: 10,
            5: 10,
        },
        null_value_counts={
            1: 0,
            2: 0,
            3: 0,
            4: 0,
            5: 0,
        },
        nan_value_counts={1: 10, 4: 10, 5: 5},
        lower_bounds={
            1: to_bytes(DoubleType(), float("nan")),
            2: to_bytes(DoubleType(), 7),
            3: to_bytes(FloatType(), float("nan")),
            5: to_bytes(FloatType(), 7),
        },
        upper_bounds={
            1: to_bytes(DoubleType(), float("nan")),
            2: to_bytes(DoubleType(), float("nan")),
            3: to_bytes(FloatType(), float("nan")),
            5: to_bytes(FloatType(), 22),
        },
    )


def test_inclusive_metrics_evaluator_less_than_and_less_than_equal(schema_data_file_nan: Schema, data_file_nan: DataFile) -> None:
    for operator in [LessThan, LessThanOrEqual]:
        should_read = _InclusiveMetricsEvaluator(schema_data_file_nan, operator("all_nan", 1)).eval(data_file_nan)
        assert not should_read, "Should not match: all nan column doesn't contain number"

        should_read = _InclusiveMetricsEvaluator(schema_data_file_nan, operator("max_nan", 1)).eval(data_file_nan)
        assert not should_read, "Should not match: 1 is smaller than lower bound"

        should_read = _InclusiveMetricsEvaluator(schema_data_file_nan, operator("max_nan", 10)).eval(data_file_nan)
        assert should_read, "Should match: 10 is larger than lower bound"

        should_read = _InclusiveMetricsEvaluator(schema_data_file_nan, operator("min_max_nan", 1)).eval(data_file_nan)
        assert should_read, "Should match: no visibility"

        should_read = _InclusiveMetricsEvaluator(schema_data_file_nan, operator("all_nan_null_bounds", 1)).eval(data_file_nan)
        assert not should_read, "Should not match: all nan column doesn't contain number"

        should_read = _InclusiveMetricsEvaluator(schema_data_file_nan, operator("some_nan_correct_bounds", 1)).eval(data_file_nan)
        assert not should_read, "Should not match: 1 is smaller than lower bound"

        should_read = _InclusiveMetricsEvaluator(schema_data_file_nan, operator("some_nan_correct_bounds", 10)).eval(
            data_file_nan
        )
        assert should_read, "Should match: 10 larger than lower bound"


def test_inclusive_metrics_evaluator_greater_than_and_greater_than_equal(
    schema_data_file_nan: Schema, data_file_nan: DataFile
) -> None:
    for operator in [GreaterThan, GreaterThanOrEqual]:
        should_read = _InclusiveMetricsEvaluator(schema_data_file_nan, operator("all_nan", 1)).eval(data_file_nan)
        assert not should_read, "Should not match: all nan column doesn't contain number"

        should_read = _InclusiveMetricsEvaluator(schema_data_file_nan, operator("max_nan", 1)).eval(data_file_nan)
        assert should_read, "Should match: upper bound is larger than 1"

        should_read = _InclusiveMetricsEvaluator(schema_data_file_nan, operator("max_nan", 10)).eval(data_file_nan)
        assert should_read, "Should match: upper bound is larger than 10"

        should_read = _InclusiveMetricsEvaluator(schema_data_file_nan, operator("min_max_nan", 1)).eval(data_file_nan)
        assert should_read, "Should match: no visibility"

        should_read = _InclusiveMetricsEvaluator(schema_data_file_nan, operator("all_nan_null_bounds", 1)).eval(data_file_nan)
        assert not should_read, "Should not match: all nan column doesn't contain number"

        should_read = _InclusiveMetricsEvaluator(schema_data_file_nan, operator("some_nan_correct_bounds", 1)).eval(data_file_nan)
        assert should_read, "Should match: 1 is smaller than upper bound"

        should_read = _InclusiveMetricsEvaluator(schema_data_file_nan, operator("some_nan_correct_bounds", 10)).eval(
            data_file_nan
        )
        assert should_read, "Should match: 10 is smaller than upper bound"

        should_read = _InclusiveMetricsEvaluator(schema_data_file_nan, operator("all_nan", 30)).eval(data_file_nan)
        assert not should_read, "Should not match: 30 is greater than upper bound"


def test_inclusive_metrics_evaluator_equals(schema_data_file_nan: Schema, data_file_nan: DataFile) -> None:
    should_read = _InclusiveMetricsEvaluator(schema_data_file_nan, EqualTo("all_nan", 1)).eval(data_file_nan)
    assert not should_read, "Should not match: all nan column doesn't contain number"

    should_read = _InclusiveMetricsEvaluator(schema_data_file_nan, EqualTo("max_nan", 1)).eval(data_file_nan)
    assert not should_read, "Should not match: 1 is smaller than lower bound"

    should_read = _InclusiveMetricsEvaluator(schema_data_file_nan, EqualTo("max_nan", 10)).eval(data_file_nan)
    assert should_read, "Should match: 10 is within bounds"

    should_read = _InclusiveMetricsEvaluator(schema_data_file_nan, EqualTo("min_max_nan", 1)).eval(data_file_nan)
    assert should_read, "Should match: no visibility"

    should_read = _InclusiveMetricsEvaluator(schema_data_file_nan, EqualTo("all_nan_null_bounds", 1)).eval(data_file_nan)
    assert not should_read, "Should not match: all nan column doesn't contain number"

    should_read = _InclusiveMetricsEvaluator(schema_data_file_nan, EqualTo("some_nan_correct_bounds", 1)).eval(data_file_nan)
    assert not should_read, "Should not match: 1 is smaller than lower bound"

    should_read = _InclusiveMetricsEvaluator(schema_data_file_nan, EqualTo("some_nan_correct_bounds", 10)).eval(data_file_nan)
    assert should_read, "Should match: 10 is within bounds"

    should_read = _InclusiveMetricsEvaluator(schema_data_file_nan, EqualTo("all_nan", 30)).eval(data_file_nan)
    assert not should_read, "Should not match: 30 is greater than upper bound"


def test_inclusive_metrics_evaluator_not_equals(schema_data_file_nan: Schema, data_file_nan: DataFile) -> None:
    should_read = _InclusiveMetricsEvaluator(schema_data_file_nan, NotEqualTo("all_nan", 1)).eval(data_file_nan)
    assert should_read, "Should match: no visibility"

    should_read = _InclusiveMetricsEvaluator(schema_data_file_nan, NotEqualTo("max_nan", 10)).eval(data_file_nan)
    assert should_read, "Should match: no visibility"

    should_read = _InclusiveMetricsEvaluator(schema_data_file_nan, NotEqualTo("max_nan", 10)).eval(data_file_nan)
    assert should_read, "Should match: no visibility"

    should_read = _InclusiveMetricsEvaluator(schema_data_file_nan, NotEqualTo("min_max_nan", 1)).eval(data_file_nan)
    assert should_read, "Should match: no visibility"

    should_read = _InclusiveMetricsEvaluator(schema_data_file_nan, NotEqualTo("all_nan_null_bounds", 1)).eval(data_file_nan)
    assert should_read, "Should match: no visibility"

    should_read = _InclusiveMetricsEvaluator(schema_data_file_nan, NotEqualTo("some_nan_correct_bounds", 1)).eval(data_file_nan)
    assert should_read, "Should match: no visibility"

    should_read = _InclusiveMetricsEvaluator(schema_data_file_nan, NotEqualTo("some_nan_correct_bounds", 10)).eval(data_file_nan)
    assert should_read, "Should match: no visibility"

    should_read = _InclusiveMetricsEvaluator(schema_data_file_nan, NotEqualTo("some_nan_correct_bounds", 30)).eval(data_file_nan)
    assert should_read, "Should match: no visibility"


def test_inclusive_metrics_evaluator_in(schema_data_file_nan: Schema, data_file_nan: DataFile) -> None:
    should_read = _InclusiveMetricsEvaluator(schema_data_file_nan, In("all_nan", (1, 10, 30))).eval(data_file_nan)
    assert not should_read, "Should not match: all nan column doesn't contain number"

    should_read = _InclusiveMetricsEvaluator(schema_data_file_nan, In("max_nan", (1, 10, 30))).eval(data_file_nan)
    assert should_read, "Should match: 10 and 30 are greater than lower bound"

    should_read = _InclusiveMetricsEvaluator(schema_data_file_nan, In("min_max_nan", (1, 10, 30))).eval(data_file_nan)
    assert should_read, "Should match: no visibility"

    should_read = _InclusiveMetricsEvaluator(schema_data_file_nan, In("all_nan_null_bounds", (1, 10, 30))).eval(data_file_nan)
    assert not should_read, "Should not match: all nan column doesn't contain number"

    should_read = _InclusiveMetricsEvaluator(schema_data_file_nan, In("some_nan_correct_bounds", (1, 10, 30))).eval(data_file_nan)
    assert should_read, "Should match: 10 within bounds"

    should_read = _InclusiveMetricsEvaluator(schema_data_file_nan, In("some_nan_correct_bounds", (1, 30))).eval(data_file_nan)
    assert not should_read, "Should not match: 1 and 30 not within bounds"

    should_read = _InclusiveMetricsEvaluator(schema_data_file_nan, In("some_nan_correct_bounds", (5, 7))).eval(data_file_nan)
    assert should_read, "Should match: overlap with lower bound"

    should_read = _InclusiveMetricsEvaluator(schema_data_file_nan, In("some_nan_correct_bounds", (22, 25))).eval(data_file_nan)
    assert should_read, "Should match: overlap with upper bounds"


def test_inclusive_metrics_evaluator_not_in(schema_data_file_nan: Schema, data_file_nan: DataFile) -> None:
    should_read = _InclusiveMetricsEvaluator(schema_data_file_nan, NotIn("all_nan", (1, 10, 30))).eval(data_file_nan)
    assert should_read, "Should match: no visibility"

    should_read = _InclusiveMetricsEvaluator(schema_data_file_nan, NotIn("max_nan", (1, 10, 30))).eval(data_file_nan)
    assert should_read, "Should match: no visibility"

    should_read = _InclusiveMetricsEvaluator(schema_data_file_nan, NotIn("max_nan", (1, 10, 30))).eval(data_file_nan)
    assert should_read, "Should match: no visibility"

    should_read = _InclusiveMetricsEvaluator(schema_data_file_nan, NotIn("min_max_nan", (1, 10, 30))).eval(data_file_nan)
    assert should_read, "Should match: no visibility"

    should_read = _InclusiveMetricsEvaluator(schema_data_file_nan, NotIn("all_nan_null_bounds", (1, 10, 30))).eval(data_file_nan)
    assert should_read, "Should match: no visibility"

    should_read = _InclusiveMetricsEvaluator(schema_data_file_nan, NotIn("some_nan_correct_bounds", (1, 30))).eval(data_file_nan)
    assert should_read, "Should match: no visibility"

    should_read = _InclusiveMetricsEvaluator(schema_data_file_nan, NotIn("some_nan_correct_bounds", (1, 30))).eval(data_file_nan)
    assert should_read, "Should match: no visibility"


def test_string_starts_with(
    schema_data_file: Schema, data_file: DataFile, data_file_2: DataFile, data_file_3: DataFile, data_file_4: DataFile
) -> None:
    should_read = _InclusiveMetricsEvaluator(schema_data_file, StartsWith("required", "a")).eval(data_file)
    assert should_read, "Should read: no stats"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, StartsWith("required", "a")).eval(data_file_2)
    assert should_read, "Should read: range matches"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, StartsWith("required", "aa")).eval(data_file_2)
    assert should_read, "Should read: range matches"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, StartsWith("required", "aaa")).eval(data_file_2)
    assert should_read, "Should read: range matches"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, StartsWith("required", "1s")).eval(data_file_3)
    assert should_read, "Should read: range matches"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, StartsWith("required", "1str1x")).eval(data_file_3)
    assert should_read, "Should read: range matches"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, StartsWith("required", "ff")).eval(data_file_4)
    assert should_read, "Should read: range matches"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, StartsWith("required", "aB")).eval(data_file_2)
    assert not should_read, "Should not read: range doesn't match"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, StartsWith("required", "dWX")).eval(data_file_2)
    assert not should_read, "Should not read: range doesn't match"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, StartsWith("required", "5")).eval(data_file_3)
    assert not should_read, "Should not read: range doesn't match"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, StartsWith("required", "3str3x")).eval(data_file_3)
    assert not should_read, "Should not read: range doesn't match"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, StartsWith("some_empty", "房东整租霍")).eval(data_file)
    assert should_read, "Should read: range matches"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, StartsWith("all_nulls", "")).eval(data_file)
    assert not should_read, "Should not read: range doesn't match"

    # above_max = UnicodeUtil.truncateStringMax(Literal.of("イロハニホヘト"), 4).value().toString();

    # should_read = _InclusiveMetricsEvaluator(schema_data_file, StartsWith("required", above_max)).eval(data_file_4)
    # assert not should_read, "Should not read: range doesn't match"


def test_string_not_starts_with(
    schema_data_file: Schema, data_file: DataFile, data_file_2: DataFile, data_file_3: DataFile, data_file_4: DataFile
) -> None:
    should_read = _InclusiveMetricsEvaluator(schema_data_file, NotStartsWith("required", "a")).eval(data_file)
    assert should_read, "Should read: no stats"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, NotStartsWith("required", "a")).eval(data_file_2)
    assert should_read, "Should read: range matches"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, NotStartsWith("required", "aa")).eval(data_file_2)
    assert should_read, "Should read: range matches"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, NotStartsWith("required", "aaa")).eval(data_file_2)
    assert should_read, "Should read: range matches"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, NotStartsWith("required", "1s")).eval(data_file_3)
    assert should_read, "Should read: range matches"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, NotStartsWith("required", "1str1x")).eval(data_file_3)
    assert should_read, "Should read: range matches"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, NotStartsWith("required", "ff")).eval(data_file_4)
    assert should_read, "Should read: range matches"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, NotStartsWith("required", "aB")).eval(data_file_2)
    assert should_read, "Should not read: range doesn't match"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, NotStartsWith("required", "dWX")).eval(data_file_2)
    assert should_read, "Should not read: range doesn't match"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, NotStartsWith("required", "5")).eval(data_file_3)
    assert should_read, "Should not read: range doesn't match"

    should_read = _InclusiveMetricsEvaluator(schema_data_file, NotStartsWith("required", "3str3x")).eval(data_file_3)
    assert should_read, "Should not read: range doesn't match"

    # above_max = UnicodeUtil.truncateStringMax(Literal.of("イロハニホヘト"), 4).value().toString();

    # should_read = _InclusiveMetricsEvaluator(schema_data_file, NotStartsWith("required", above_max)).eval(data_file_4)
    # assert should_read, "Should not read: range doesn't match"
