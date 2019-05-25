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

from iceberg.api.expressions import (And,
                                     Binder,
                                     Expressions,
                                     Not,
                                     Or)
from iceberg.api.types import (IntegerType,
                               NestedField,
                               StructType)
import iceberg.exceptions as ice_ex
from pytest import raises

STRUCT = StructType.of([NestedField.required(0, "x", IntegerType.get()),
                        NestedField.required(1, "y", IntegerType.get()),
                        NestedField.required(2, "z", IntegerType.get())])


def test_missing_reference():
    expr = Expressions.and_(Expressions.equal("t", 5),
                            Expressions.equal("x", 7))
    try:
        Binder.bind(STRUCT, expr)
    except ice_ex.ValidationException as e:
        assert "Cannot find field 't' in struct" in "{}".format(e)


def test_bound_expression_fails():
    with raises(RuntimeError):
        expr = Expressions.not_(Expressions.equal("x", 7))
        Binder.bind(STRUCT,
                    Binder.bind(STRUCT, expr))


def test_single_reference(assert_all_bound):
    expr = Expressions.not_(Expressions.equal("x", 7))
    assert_all_bound("Single reference",
                     Binder.bind(STRUCT, expr))


def test_case_insensitive_reference(assert_all_bound):
    expr = Expressions.not_(Expressions.equal("X", 7))
    assert_all_bound("Single reference",
                     Binder.bind(STRUCT, expr, case_sensitive=False))


def test_case_sensitive_reference():
    with raises(ice_ex.ValidationException):
        expr = Expressions.not_(Expressions.equal("X", 7))
        Binder.bind(STRUCT, expr, case_sensitive=True)


def test_multiple_references(assert_all_bound):
    expr = Expressions.or_(Expressions.and_(Expressions.equal("x", 7),
                                            Expressions.less_than("y", 100)),
                           Expressions.greater_than("z", -100))

    assert_all_bound("Multiple references", Binder.bind(STRUCT, expr))


def test_and(assert_all_bound, assert_and_unwrap):
    expr = Expressions.and_(Expressions.equal("x", 7),
                            Expressions.less_than("y", 100))
    bound_expr = Binder.bind(STRUCT, expr)
    assert_all_bound("And", bound_expr)

    and_ = assert_and_unwrap(bound_expr, And)

    left = assert_and_unwrap(and_.left, None)
    # should bind x correctly
    assert 0 == left.ref.field_id
    right = assert_and_unwrap(and_.right, None)
    # should bind y correctly
    assert 1 == right.ref.field_id


def test_or(assert_all_bound, assert_and_unwrap):
    expr = Expressions.or_(Expressions.greater_than("z", -100),
                           Expressions.less_than("y", 100))
    bound_expr = Binder.bind(STRUCT, expr)
    assert_all_bound("Or", bound_expr)

    or_ = assert_and_unwrap(bound_expr, Or)

    left = assert_and_unwrap(or_.left, None)
    # should bind z correctly
    assert 2 == left.ref.field_id
    right = assert_and_unwrap(or_.right, None)
    # should bind y correctly
    assert 1 == right.ref.field_id


def test_not(assert_all_bound, assert_and_unwrap):
    expr = Expressions.not_(Expressions.equal("x", 7))
    bound_expr = Binder.bind(STRUCT, expr)
    assert_all_bound("Not", bound_expr)

    not_ = assert_and_unwrap(bound_expr, Not)

    child = assert_and_unwrap(not_.child, None)
    # should bind x correctly
    assert 0 == child.ref.field_id


def test_always_true():
    assert Expressions.always_true() == Binder.bind(STRUCT,
                                                    Expressions.always_true())


def test_always_false():
    assert Expressions.always_false() == Binder.bind(STRUCT,
                                                     Expressions.always_false())


def test_basic_simplification(assert_and_unwrap):
    # Should simplify or expression to alwaysTrue
    assert Expressions.always_true() == Binder.bind(STRUCT,
                                                    Expressions.or_(Expressions.less_than("y", 100),
                                                                    Expressions.greater_than("z", -9999999999)))
    # Should simplify or expression to alwaysfalse
    assert Expressions.always_false() == Binder.bind(STRUCT,
                                                     Expressions.and_(Expressions.less_than("y", 100),
                                                                      Expressions.less_than("z", -9999999999)))

    bound = Binder.bind(STRUCT,
                        Expressions.not_(Expressions.not_(Expressions.less_than("y", 100))))
    pred = assert_and_unwrap(bound, None)
    assert 1 == pred.ref.field_id
