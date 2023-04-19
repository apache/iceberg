#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.
# pylint:disable=redefined-outer-name

import pytest

from pyiceberg.expressions import (
    AlwaysTrue,
    And,
    EqualTo,
    GreaterThan,
    GreaterThanOrEqual,
    In,
    IsNull,
    LessThan,
    LessThanOrEqual,
    Not,
    NotEqualTo,
    NotIn,
    NotNull,
    Or,
)
from pyiceberg.expressions.visitors import inclusive_projection
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import (
    BucketTransform,
    DayTransform,
    HourTransform,
    IdentityTransform,
    TruncateTransform,
)
from pyiceberg.types import (
    DateType,
    LongType,
    NestedField,
    StringType,
    TimestampType,
)


@pytest.fixture
def schema() -> Schema:
    return Schema(
        NestedField(1, "id", LongType(), required=False),
        NestedField(2, "data", StringType(), required=False),
        NestedField(3, "event_date", DateType(), required=False),
        NestedField(4, "event_ts", TimestampType(), required=False),
    )


@pytest.fixture
def empty_spec() -> PartitionSpec:
    return PartitionSpec()


@pytest.fixture
def id_spec() -> PartitionSpec:
    return PartitionSpec(PartitionField(1, 1000, IdentityTransform(), "id_part"))


@pytest.fixture
def bucket_spec() -> PartitionSpec:
    return PartitionSpec(PartitionField(2, 1000, BucketTransform(16), "data_bucket"))


@pytest.fixture
def day_spec() -> PartitionSpec:
    return PartitionSpec(PartitionField(4, 1000, DayTransform(), "date"), PartitionField(3, 1000, DayTransform(), "ddate"))


@pytest.fixture
def hour_spec() -> PartitionSpec:
    return PartitionSpec(PartitionField(4, 1000, HourTransform(), "hour"))


@pytest.fixture
def truncate_str_spec() -> PartitionSpec:
    return PartitionSpec(PartitionField(2, 1000, TruncateTransform(2), "data_trunc"))


@pytest.fixture
def truncate_int_spec() -> PartitionSpec:
    return PartitionSpec(PartitionField(1, 1000, TruncateTransform(10), "id_trunc"))


@pytest.fixture
def id_and_bucket_spec() -> PartitionSpec:
    return PartitionSpec(
        PartitionField(1, 1000, IdentityTransform(), "id_part"), PartitionField(2, 1001, BucketTransform(16), "data_bucket")
    )


def test_identity_projection(schema: Schema, id_spec: PartitionSpec) -> None:
    predicates = [
        NotNull("id"),
        IsNull("id"),
        LessThan("id", 100),
        LessThanOrEqual("id", 101),
        GreaterThan("id", 102),
        GreaterThanOrEqual("id", 103),
        EqualTo("id", 104),
        NotEqualTo("id", 105),
        In("id", {3, 4, 5}),
        NotIn("id", {3, 4, 5}),
    ]

    expected = [
        NotNull("id_part"),
        IsNull("id_part"),
        LessThan("id_part", 100),
        LessThanOrEqual("id_part", 101),
        GreaterThan("id_part", 102),
        GreaterThanOrEqual("id_part", 103),
        EqualTo("id_part", 104),
        NotEqualTo("id_part", 105),
        In("id_part", {3, 4, 5}),
        NotIn("id_part", {3, 4, 5}),
    ]

    project = inclusive_projection(schema, id_spec)
    for index, predicate in enumerate(predicates):
        expr = project(predicate)
        assert expected[index] == expr


def test_bucket_projection(schema: Schema, bucket_spec: PartitionSpec) -> None:
    predicates = [
        NotNull("data"),
        IsNull("data"),
        LessThan("data", "val"),
        LessThanOrEqual("data", "val"),
        GreaterThan("data", "val"),
        GreaterThanOrEqual("data", "val"),
        EqualTo("data", "val"),
        NotEqualTo("data", "val"),
        In("data", {"v1", "v2", "v3"}),
        NotIn("data", {"v1", "v2", "v3"}),
    ]

    expected = [
        NotNull("data_bucket"),
        IsNull("data_bucket"),
        AlwaysTrue(),
        AlwaysTrue(),
        AlwaysTrue(),
        AlwaysTrue(),
        EqualTo("data_bucket", 14),
        AlwaysTrue(),
        In("data_bucket", {1, 3, 13}),
        AlwaysTrue(),
    ]

    project = inclusive_projection(schema, bucket_spec)
    for index, predicate in enumerate(predicates):
        expr = project(predicate)
        assert expected[index] == expr


def test_hour_projection(schema: Schema, hour_spec: PartitionSpec) -> None:
    predicates = [
        NotNull("event_ts"),
        IsNull("event_ts"),
        LessThan("event_ts", "2022-11-27T10:00:00"),
        LessThanOrEqual("event_ts", "2022-11-27T10:00:00"),
        GreaterThan("event_ts", "2022-11-27T09:59:59.999999"),
        GreaterThanOrEqual("event_ts", "2022-11-27T09:59:59.999999"),
        EqualTo("event_ts", "2022-11-27T10:00:00"),
        NotEqualTo("event_ts", "2022-11-27T10:00:00"),
        In("event_ts", {"2022-11-27T10:00:00", "2022-11-27T09:59:59.999999"}),
        NotIn("event_ts", {"2022-11-27T10:00:00", "2022-11-27T09:59:59.999999"}),
    ]

    expected = [
        NotNull("hour"),
        IsNull("hour"),
        LessThanOrEqual("hour", 463761),
        LessThanOrEqual("hour", 463762),
        GreaterThanOrEqual("hour", 463762),
        GreaterThanOrEqual("hour", 463761),
        EqualTo("hour", 463762),
        AlwaysTrue(),
        In("hour", {463761, 463762}),
        AlwaysTrue(),
    ]

    project = inclusive_projection(schema, hour_spec)
    for index, predicate in enumerate(predicates):
        expr = project(predicate)
        assert expected[index] == expr, predicate


def test_day_projection(schema: Schema, day_spec: PartitionSpec) -> None:
    predicates = [
        NotNull("event_ts"),
        IsNull("event_ts"),
        LessThan("event_ts", "2022-11-27T00:00:00"),
        LessThanOrEqual("event_ts", "2022-11-27T00:00:00"),
        GreaterThan("event_ts", "2022-11-26T23:59:59.999999"),
        GreaterThanOrEqual("event_ts", "2022-11-26T23:59:59.999999"),
        EqualTo("event_ts", "2022-11-27T10:00:00"),
        NotEqualTo("event_ts", "2022-11-27T10:00:00"),
        In("event_ts", {"2022-11-27T00:00:00", "2022-11-26T23:59:59.999999"}),
        NotIn("event_ts", {"2022-11-27T00:00:00", "2022-11-26T23:59:59.999999"}),
    ]

    expected = [
        NotNull("date"),
        IsNull("date"),
        LessThanOrEqual("date", 19322),
        LessThanOrEqual("date", 19323),
        GreaterThanOrEqual("date", 19323),
        GreaterThanOrEqual("date", 19322),
        EqualTo("date", 19323),
        AlwaysTrue(),
        In("date", {19322, 19323}),
        AlwaysTrue(),
    ]

    project = inclusive_projection(schema, day_spec)
    for index, predicate in enumerate(predicates):
        expr = project(predicate)
        assert expected[index] == expr, predicate


def test_date_day_projection(schema: Schema, day_spec: PartitionSpec) -> None:
    predicates = [
        NotNull("event_date"),
        IsNull("event_date"),
        LessThan("event_date", "2022-11-27"),
        LessThanOrEqual("event_date", "2022-11-27"),
        GreaterThan("event_date", "2022-11-26"),
        GreaterThanOrEqual("event_date", "2022-11-26"),
        EqualTo("event_date", "2022-11-27"),
        NotEqualTo("event_date", "2022-11-27"),
        In("event_date", {"2022-11-26", "2022-11-27"}),
        NotIn("event_date", {"2022-11-26", "2022-11-27"}),
    ]

    expected = [
        NotNull("ddate"),
        IsNull("ddate"),
        LessThanOrEqual("ddate", 19322),
        LessThanOrEqual("ddate", 19323),
        GreaterThanOrEqual("ddate", 19323),
        GreaterThanOrEqual("ddate", 19322),
        EqualTo("ddate", 19323),
        AlwaysTrue(),
        In("ddate", {19322, 19323}),
        AlwaysTrue(),
    ]

    project = inclusive_projection(schema, day_spec)
    for index, predicate in enumerate(predicates):
        expr = project(predicate)
        assert expected[index] == expr, predicate


def test_string_truncate_projection(schema: Schema, truncate_str_spec: PartitionSpec) -> None:
    predicates = [
        NotNull("data"),
        IsNull("data"),
        LessThan("data", "aaa"),
        LessThanOrEqual("data", "aaa"),
        GreaterThan("data", "aaa"),
        GreaterThanOrEqual("data", "aaa"),
        EqualTo("data", "aaa"),
        NotEqualTo("data", "aaa"),
        In("data", {"aaa", "aab"}),
        NotIn("data", {"aaa", "aab"}),
    ]

    expected = [
        NotNull("data_trunc"),
        IsNull("data_trunc"),
        LessThanOrEqual("data_trunc", "aa"),
        LessThanOrEqual("data_trunc", "aa"),
        GreaterThanOrEqual("data_trunc", "aa"),
        GreaterThanOrEqual("data_trunc", "aa"),
        EqualTo("data_trunc", "aa"),
        AlwaysTrue(),
        EqualTo("data_trunc", "aa"),
        AlwaysTrue(),
    ]

    project = inclusive_projection(schema, truncate_str_spec)
    for index, predicate in enumerate(predicates):
        expr = project(predicate)
        assert expected[index] == expr, predicate


def test_int_truncate_projection(schema: Schema, truncate_int_spec: PartitionSpec) -> None:
    predicates = [
        NotNull("id"),
        IsNull("id"),
        LessThan("id", 10),
        LessThanOrEqual("id", 10),
        GreaterThan("id", 9),
        GreaterThanOrEqual("id", 10),
        EqualTo("id", 15),
        NotEqualTo("id", 15),
        In("id", {15, 16}),
        NotIn("id", {15, 16}),
    ]

    expected = [
        NotNull("id_trunc"),
        IsNull("id_trunc"),
        LessThanOrEqual("id_trunc", 0),
        LessThanOrEqual("id_trunc", 10),
        GreaterThanOrEqual("id_trunc", 10),
        GreaterThanOrEqual("id_trunc", 10),
        EqualTo("id_trunc", 10),
        AlwaysTrue(),
        EqualTo("id_trunc", 10),
        AlwaysTrue(),
    ]

    project = inclusive_projection(schema, truncate_int_spec)
    for index, predicate in enumerate(predicates):
        expr = project(predicate)
        assert expected[index] == expr, predicate


def test_projection_case_sensitive(schema: Schema, id_spec: PartitionSpec) -> None:
    project = inclusive_projection(schema, id_spec)
    with pytest.raises(ValueError) as exc_info:
        project(NotNull("ID"))
        assert str(exc_info) == "Could not find field with name ID, case_sensitive=True"


def test_projection_case_insensitive(schema: Schema, id_spec: PartitionSpec) -> None:
    project = inclusive_projection(schema, id_spec, case_sensitive=False)
    assert NotNull("id_part") == project(NotNull("ID"))


def test_projection_empty_spec(schema: Schema, empty_spec: PartitionSpec) -> None:
    project = inclusive_projection(schema, empty_spec)
    assert AlwaysTrue() == project(And(LessThan("id", 5), NotNull("data")))


def test_and_projection_multiple_projected_fields(schema: Schema, id_and_bucket_spec: PartitionSpec) -> None:
    project = inclusive_projection(schema, id_and_bucket_spec)
    assert project(And(LessThan("id", 5), In("data", {"a", "b", "c"}))) == And(
        LessThan("id_part", 5), In("data_bucket", {2, 3, 15})
    )


def test_or_projection_multiple_projected_fields(schema: Schema, id_and_bucket_spec: PartitionSpec) -> None:
    project = inclusive_projection(schema, id_and_bucket_spec)
    assert project(Or(LessThan("id", 5), In("data", {"a", "b", "c"}))) == Or(
        LessThan("id_part", 5), In("data_bucket", {2, 3, 15})
    )


def test_not_projection_multiple_projected_fields(schema: Schema, id_and_bucket_spec: PartitionSpec) -> None:
    project = inclusive_projection(schema, id_and_bucket_spec)
    # Not causes In to be rewritten to NotIn, which cannot be projected
    assert project(Not(Or(LessThan("id", 5), In("data", {"a", "b", "c"})))) == GreaterThanOrEqual("id_part", 5)


def test_projection_partial_projected_fields(schema: Schema, id_spec: PartitionSpec) -> None:
    project = inclusive_projection(schema, id_spec)
    assert project(And(LessThan("id", 5), In("data", {"a", "b", "c"}))) == LessThan("id_part", 5)
