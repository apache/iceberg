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
from typing import Any, List

from pyiceberg.expressions import (
    AlwaysFalse,
    AlwaysTrue,
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
    Or,
)
from pyiceberg.expressions.visitors import expression_evaluator
from pyiceberg.schema import Schema
from pyiceberg.typedef import StructProtocol
from pyiceberg.types import (
    DoubleType,
    LongType,
    NestedField,
    StringType,
)


class Record(StructProtocol):
    data: List[Any]

    def __init__(self, *values: Any) -> None:
        self.data = list(values)

    def get(self, pos: int) -> Any:
        return self.data[pos]

    def set(self, pos: int, value: Any) -> None:
        self.data[pos] = value


SIMPLE_SCHEMA = Schema(
    NestedField(id=1, name="id", field_type=LongType()), NestedField(id=2, name="data", field_type=StringType(), required=False)
)

FLOAT_SCHEMA = Schema(
    NestedField(id=1, name="id", field_type=LongType()), NestedField(id=2, name="f", field_type=DoubleType(), required=False)
)


def test_true() -> None:
    evaluate = expression_evaluator(SIMPLE_SCHEMA, AlwaysTrue(), case_sensitive=True)
    assert evaluate(Record(1, "a"))


def test_false() -> None:
    evaluate = expression_evaluator(SIMPLE_SCHEMA, AlwaysFalse(), case_sensitive=True)
    assert not evaluate(Record(1, "a"))


def test_less_than() -> None:
    evaluate = expression_evaluator(SIMPLE_SCHEMA, LessThan("id", 3), case_sensitive=True)
    assert evaluate(Record(2, "a"))
    assert not evaluate(Record(3, "a"))


def test_less_than_or_equal() -> None:
    evaluate = expression_evaluator(SIMPLE_SCHEMA, LessThanOrEqual("id", 3), case_sensitive=True)
    assert evaluate(Record(1, "a"))
    assert evaluate(Record(3, "a"))
    assert not evaluate(Record(4, "a"))


def test_greater_than() -> None:
    evaluate = expression_evaluator(SIMPLE_SCHEMA, GreaterThan("id", 3), case_sensitive=True)
    assert not evaluate(Record(1, "a"))
    assert not evaluate(Record(3, "a"))
    assert evaluate(Record(4, "a"))


def test_greater_than_or_equal() -> None:
    evaluate = expression_evaluator(SIMPLE_SCHEMA, GreaterThanOrEqual("id", 3), case_sensitive=True)
    assert not evaluate(Record(2, "a"))
    assert evaluate(Record(3, "a"))
    assert evaluate(Record(4, "a"))


def test_equal_to() -> None:
    evaluate = expression_evaluator(SIMPLE_SCHEMA, EqualTo("id", 3), case_sensitive=True)
    assert not evaluate(Record(2, "a"))
    assert evaluate(Record(3, "a"))
    assert not evaluate(Record(4, "a"))


def test_not_equal_to() -> None:
    evaluate = expression_evaluator(SIMPLE_SCHEMA, NotEqualTo("id", 3), case_sensitive=True)
    assert evaluate(Record(2, "a"))
    assert not evaluate(Record(3, "a"))
    assert evaluate(Record(4, "a"))


def test_in() -> None:
    evaluate = expression_evaluator(SIMPLE_SCHEMA, In("id", [1, 2, 3]), case_sensitive=True)
    assert evaluate(Record(2, "a"))
    assert evaluate(Record(3, "a"))
    assert not evaluate(Record(4, "a"))


def test_not_in() -> None:
    evaluate = expression_evaluator(SIMPLE_SCHEMA, NotIn("id", [1, 2, 3]), case_sensitive=True)
    assert not evaluate(Record(2, "a"))
    assert not evaluate(Record(3, "a"))
    assert evaluate(Record(4, "a"))


def test_is_null() -> None:
    evaluate = expression_evaluator(SIMPLE_SCHEMA, IsNull("data"), case_sensitive=True)
    assert not evaluate(Record(2, "a"))
    assert evaluate(Record(3, None))


def test_not_null() -> None:
    evaluate = expression_evaluator(SIMPLE_SCHEMA, NotNull("data"), case_sensitive=True)
    assert evaluate(Record(2, "a"))
    assert not evaluate(Record(3, None))


def test_is_nan() -> None:
    evaluate = expression_evaluator(FLOAT_SCHEMA, IsNaN("f"), case_sensitive=True)
    assert not evaluate(Record(2, 0.0))
    assert not evaluate(Record(3, float("infinity")))
    assert evaluate(Record(4, float("nan")))


def test_not_nan() -> None:
    evaluate = expression_evaluator(FLOAT_SCHEMA, NotNaN("f"), case_sensitive=True)
    assert evaluate(Record(2, 0.0))
    assert evaluate(Record(3, float("infinity")))
    assert not evaluate(Record(4, float("nan")))


def test_not() -> None:
    evaluate = expression_evaluator(SIMPLE_SCHEMA, Not(LessThan("id", 3)), case_sensitive=True)
    assert not evaluate(Record(2, "a"))
    assert evaluate(Record(3, "a"))


def test_and() -> None:
    evaluate = expression_evaluator(SIMPLE_SCHEMA, And(LessThan("id", 3), GreaterThan("id", 1)), case_sensitive=True)
    assert not evaluate(Record(1, "a"))
    assert evaluate(Record(2, "a"))
    assert not evaluate(Record(3, "a"))


def test_or() -> None:
    evaluate = expression_evaluator(SIMPLE_SCHEMA, Or(LessThan("id", 2), GreaterThan("id", 2)), case_sensitive=True)
    assert evaluate(Record(1, "a"))
    assert not evaluate(Record(2, "a"))
    assert evaluate(Record(3, "a"))
