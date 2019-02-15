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

from decimal import Decimal
import io
import pickle
import uuid

from iceberg.api import DataFile
from iceberg.api.expressions import (BoundPredicate,
                                     Expressions,
                                     ExpressionVisitors,
                                     Literal,
                                     Operation,
                                     UnboundPredicate)
from iceberg.api.schema import Schema
from iceberg.api.struct_like import StructLike
from iceberg.api.types import (BinaryType,
                               Conversions,
                               DateType,
                               DecimalType,
                               FixedType,
                               IntegerType,
                               NestedField,
                               StringType,
                               TimestampType,
                               TimeType)
import pytest

exp_schema = Schema(NestedField.optional(34, "a", IntegerType.get()))


class TestHelpers(object):

    @staticmethod
    def assert_all_references_bound(message, expr):
        ExpressionVisitors.visit(expr, TestHelpers.CheckReferencesBound(message))

    @staticmethod
    def assert_and_unwrap(expr, expected=None):
        if expected is not None:
            assert isinstance(expr, expected)
        else:
            assert isinstance(expr, BoundPredicate)

        return expr

    @staticmethod
    def round_trip_serialize(type_var):
        stream = io.BytesIO()
        pickle.dump(type_var, stream, pickle.HIGHEST_PROTOCOL)
        stream.seek(0)

        return pickle.load(stream)

    class Row(StructLike):

        @staticmethod
        def of(values=None):
            return TestHelpers.Row(values)

        def __init__(self, values):
            self.values = values

        def get(self, pos):
            return self.values[pos]

        def set(self, pos, value):
            raise RuntimeError("Setting values is not supported")

    class CheckReferencesBound(ExpressionVisitors.ExpressionVisitor):

        def __init__(self, message):
            self.message = message

        def predicate(self, pred):
            if isinstance(pred, UnboundPredicate):
                assert False


class TestDataFile(DataFile):

    def __init__(self, path, partition, record_count, value_counts=None, null_value_counts=None,
                 lower_bounds=None, upper_bounds=None):
        self.path = path
        self.partition = partition
        self.record_count = record_count
        self.value_counts = value_counts
        self.null_value_counts = null_value_counts
        self.lower_bounds = lower_bounds
        self.upper_bounds = upper_bounds
        self.file_size_in_bytes = 0
        self.block_size_in_bytes = 0
        self.file_ordinal = None
        self.column_size = None

    def copy(self):
        return self


@pytest.fixture(scope="session")
def schema():
    return Schema(NestedField.required(1, "id", IntegerType.get()),
                  NestedField.optional(2, "no_stats", IntegerType.get()),
                  NestedField.required(3, "required", StringType.get()),
                  NestedField.optional(4, "all_nulls", StringType.get()),
                  NestedField.optional(5, "some_nulls", StringType.get()),
                  NestedField.optional(6, "no_nulls", StringType.get()))


@pytest.fixture(scope="session")
def strict_schema():
    return Schema(NestedField.required(1, "id", IntegerType.get()),
                  NestedField.optional(2, "no_stats", IntegerType.get()),
                  NestedField.required(3, "required", StringType.get()),
                  NestedField.optional(4, "all_nulls", StringType.get()),
                  NestedField.optional(5, "some_nulls", StringType.get()),
                  NestedField.optional(6, "no_nulls", StringType.get()),
                  NestedField.required(7, "always_5", IntegerType.get()))


@pytest.fixture(scope="session")
def file():
    return TestDataFile("file.avro", TestHelpers.Row.of(), 50,
                        # value counts
                        {4: 50, 5: 50, 6: 50},
                        # null value counts
                        {4: 50, 5: 10, 6: 0},
                        # lower bounds
                        {1: Conversions.to_byte_buffer(IntegerType.get(), 30)},
                        # upper bounds
                        {1: Conversions.to_byte_buffer(IntegerType.get(), 79)})


@pytest.fixture(scope="session")
def strict_file():
    return TestDataFile("file.avro",
                        TestHelpers.Row.of(),
                        50,
                        {4: 50, 5: 50, 6: 50},
                        {4: 50, 5: 10, 6: 0},
                        {1: Conversions.to_byte_buffer(IntegerType.get(), 30),
                         7: Conversions.to_byte_buffer(IntegerType.get(), 5)},
                        {1: Conversions.to_byte_buffer(IntegerType.get(), 79),
                         7: Conversions.to_byte_buffer(IntegerType.get(), 5)}
                        )


@pytest.fixture(scope="session")
def missing_stats():
    return TestDataFile("file.parquet", TestHelpers.Row.of(), 50)


@pytest.fixture(scope="session")
def empty():
    return TestDataFile("file.parquet", TestHelpers.Row.of(), record_count=0)


@pytest.fixture(scope="session")
def assert_and_unwrap():
    return lambda x, y=None: TestHelpers.assert_and_unwrap(x, y)


@pytest.fixture(scope="session")
def assert_all_bound():
    return lambda msg, expr: TestHelpers.assert_all_references_bound(msg, expr)


@pytest.fixture(scope="session")
def round_trip_serialize():
    return lambda x: TestHelpers.round_trip_serialize(x)


@pytest.fixture(scope="session")
def row_of():
    return lambda x: TestHelpers.Row.of(x)


@pytest.fixture(scope="session",
                params=[Operation.LT,
                        Operation.LT_EQ,
                        Operation.GT,
                        Operation.GT_EQ,
                        Operation.EQ,
                        Operation.NOT_EQ])
def op(request):
    yield request.param


@pytest.fixture(scope="session",
                params=[Expressions.always_false(),
                        Expressions.always_true(),
                        Expressions.less_than("x", 5),
                        Expressions.less_than_or_equal("y", -3),
                        Expressions.greater_than("z", 0),
                        Expressions.greater_than_or_equal("t", 129),
                        Expressions.equal("col", "data"),
                        Expressions.not_equal("col", "abc"),
                        Expressions.not_null("maybeNull"),
                        Expressions.is_null("maybeNull2"),
                        Expressions.not_(Expressions.greater_than("a", 10)),
                        Expressions.and_(Expressions.greater_than_or_equal("a", 0),
                                         Expressions.less_than("a", 3)),
                        Expressions.or_(Expressions.less_than("a", 0),
                                        Expressions.greater_than("a", 10)),
                        Expressions.equal("a", 5).bind(exp_schema.as_struct())])
def expression(request):
    yield request.param


@pytest.fixture(scope="session",
                params=[Expressions.less_than("no_stats", 5),
                        Expressions.less_than_or_equal("no_stats", 30),
                        Expressions.equal("no_stats", 70),
                        Expressions.greater_than("no_stats", 78),
                        Expressions.greater_than_or_equal("no_stats", 90),
                        Expressions.not_equal("no_stats", 101),
                        Expressions.is_null("no_stats"),
                        Expressions.not_null("no_stats")])
def missing_stats_exprs(request):
    yield request.param


@pytest.fixture(scope="session",
                params=[Expressions.less_than("id", 5),
                        Expressions.less_than_or_equal("id", 30),
                        Expressions.equal("id", 70),
                        Expressions.greater_than("id", 78),
                        Expressions.greater_than_or_equal("id", 90),
                        Expressions.not_equal("id", 101),
                        Expressions.is_null("some_nulls"),
                        Expressions.not_null("some_nulls")])
def zero_rows_exprs(request):
    yield request.param


@pytest.fixture(scope="session",
                params=[Expressions.not_equal("id", 5),
                        Expressions.not_equal("id", 29),
                        Expressions.not_equal("id", 30),
                        Expressions.not_equal("id", 75),
                        Expressions.not_equal("id", 79),
                        Expressions.not_equal("id", 80),
                        Expressions.not_equal("id", 85)])
def not_eq(request):
    yield request.param


@pytest.fixture(scope="session",
                params=[Expressions.equal("id", 5),
                        Expressions.equal("id", 29),
                        Expressions.equal("id", 30),
                        Expressions.equal("id", 75),
                        Expressions.equal("id", 79),
                        Expressions.equal("id", 80),
                        Expressions.equal("id", 85)])
def not_eq_rewrite(request):
    yield request.param


@pytest.fixture(scope="session",
                params=[Literal.of(False),
                        Literal.of(34),
                        Literal.of(35),
                        Literal.of(36.75),
                        Literal.of(8.75),
                        Literal.of("2017-11-29").to(DateType.get()),
                        Literal.of("11:30:0").to(TimeType.get()),
                        Literal.of("2017-11-29T11:30:07.123").to(TimestampType.without_timezone()),
                        Literal.of("2017-11-29T11:30:07.123+01:00").to(TimestampType.with_timezone()),
                        Literal.of("abc"),
                        Literal.of(uuid.uuid4()),
                        Literal.of(bytes([0x01, 0x02, 0x03])).to(FixedType.of_length(3)),
                        Literal.of(bytes([0x03, 0x04, 0x05, 0x06])).to(BinaryType.get()),
                        Literal.of(Decimal(122.50).quantize(Decimal(".01")))])
def literal(request):
    yield request.param


@pytest.fixture(scope="session",
                params=[(DecimalType.of(9, 0), "34"),
                        (DecimalType.of(9, 2), "34.00"),
                        (DecimalType.of(9, 4), "34.0000")])
def type_val_tuples(request):
    yield request.param


@pytest.fixture(scope="session",
                params=[(DecimalType.of(9, 1), "34.6"),
                        (DecimalType.of(9, 2), "34.56"),
                        (DecimalType.of(9, 4), "34.5600")])
def float_type_val_tuples(request):
    yield request.param
