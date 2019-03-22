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

from iceberg.api.expressions import (Expressions,
                                     StrictMetricsEvaluator)
from iceberg.exceptions import ValidationException
from pytest import raises


def test_all_nulls(strict_schema, strict_file):
    assert not StrictMetricsEvaluator(strict_schema, Expressions.not_null("all_nulls")).eval(strict_file)
    assert not StrictMetricsEvaluator(strict_schema, Expressions.not_null("some_nulls")).eval(strict_file)
    assert StrictMetricsEvaluator(strict_schema, Expressions.not_null("no_nulls")).eval(strict_file)


def test_no_nulls(strict_schema, strict_file):
    assert StrictMetricsEvaluator(strict_schema, Expressions.is_null("all_nulls")).eval(strict_file)
    assert not StrictMetricsEvaluator(strict_schema, Expressions.is_null("some_nulls")).eval(strict_file)
    assert not StrictMetricsEvaluator(strict_schema, Expressions.is_null("no_nulls")).eval(strict_file)


def test_required_columns(strict_schema, strict_file):
    assert StrictMetricsEvaluator(strict_schema, Expressions.not_null("required")).eval(strict_file)
    assert not StrictMetricsEvaluator(strict_schema, Expressions.is_null("required")).eval(strict_file)


def test_missing_column(strict_schema, strict_file):
    with raises(ValidationException):
        StrictMetricsEvaluator(strict_schema, Expressions.less_than("missing", 5)).eval(strict_file)


def test_missing_stats(strict_schema, missing_stats):
    exprs = [Expressions.less_than("no_stats", 5),
             Expressions.less_than_or_equal("no_stats", 30),
             Expressions.equal("no_stats", 70),
             Expressions.greater_than("no_stats", 78),
             Expressions.greater_than_or_equal("no_stats", 90),
             Expressions.not_equal("no_stats", 101),
             Expressions.is_null("no_stats"),
             Expressions.not_null("no_stats")]

    for expr in exprs:
        assert not StrictMetricsEvaluator(strict_schema, expr).eval(missing_stats)


def test_zero_record_file(strict_schema, empty):

    exprs = [Expressions.less_than("no_stats", 5),
             Expressions.less_than_or_equal("no_stats", 30),
             Expressions.equal("no_stats", 70),
             Expressions.greater_than("no_stats", 78),
             Expressions.greater_than_or_equal("no_stats", 90),
             Expressions.not_equal("no_stats", 101),
             Expressions.is_null("no_stats"),
             Expressions.not_null("no_stats")]
    for expr in exprs:
        assert StrictMetricsEvaluator(strict_schema, expr).eval(empty)


def test_not(strict_schema, strict_file):
    assert StrictMetricsEvaluator(strict_schema,
                                  Expressions.not_(Expressions.less_than("id", 5))).eval(strict_file)
    assert not StrictMetricsEvaluator(strict_schema,
                                      Expressions.not_(Expressions.greater_than("id", 5))).eval(strict_file)


def test_and(strict_schema, strict_file):
    assert not StrictMetricsEvaluator(strict_schema,
                                      Expressions.and_(Expressions.greater_than("id", 5),
                                                       Expressions.less_than_or_equal("id", 30))).eval(strict_file)
    assert not StrictMetricsEvaluator(strict_schema,
                                      Expressions.and_(Expressions.less_than("id", 5),
                                                       Expressions.greater_than_or_equal("id", 0))).eval(strict_file)
    assert StrictMetricsEvaluator(strict_schema,
                                  Expressions.and_(Expressions.less_than("id", 85),
                                                   Expressions.greater_than_or_equal("id", 0))).eval(strict_file)


def test_or(strict_schema, strict_file):
    assert not StrictMetricsEvaluator(strict_schema,
                                      Expressions.or_(Expressions.less_than("id", 5),
                                                      Expressions.greater_than_or_equal("id", 80))).eval(strict_file)
    assert not StrictMetricsEvaluator(strict_schema,
                                      Expressions.or_(Expressions.less_than("id", 5),
                                                      Expressions.greater_than_or_equal("id", 60))).eval(strict_file)
    assert StrictMetricsEvaluator(strict_schema,
                                  Expressions.or_(Expressions.less_than("id", 5),
                                                  Expressions.greater_than_or_equal("id", 30))).eval(strict_file)


def test_integer_lt(strict_schema, strict_file):
    assert not StrictMetricsEvaluator(strict_schema, Expressions.less_than("id", 5)).eval(strict_file)
    assert not StrictMetricsEvaluator(strict_schema, Expressions.less_than("id", 31)).eval(strict_file)
    assert not StrictMetricsEvaluator(strict_schema, Expressions.less_than("id", 79)).eval(strict_file)
    assert StrictMetricsEvaluator(strict_schema, Expressions.less_than("id", 80)).eval(strict_file)


def test_integer_lt_eq(strict_schema, strict_file):
    assert not StrictMetricsEvaluator(strict_schema, Expressions.less_than_or_equal("id", 29)).eval(strict_file)
    assert not StrictMetricsEvaluator(strict_schema, Expressions.less_than_or_equal("id", 30)).eval(strict_file)
    assert StrictMetricsEvaluator(strict_schema, Expressions.less_than_or_equal("id", 79)).eval(strict_file)
    assert StrictMetricsEvaluator(strict_schema, Expressions.less_than_or_equal("id", 80)).eval(strict_file)


def test_integer_gt(strict_schema, strict_file):
    assert not StrictMetricsEvaluator(strict_schema, Expressions.greater_than("id", 79)).eval(strict_file)
    assert not StrictMetricsEvaluator(strict_schema, Expressions.greater_than("id", 78)).eval(strict_file)
    assert not StrictMetricsEvaluator(strict_schema, Expressions.greater_than("id", 30)).eval(strict_file)
    assert StrictMetricsEvaluator(strict_schema, Expressions.greater_than("id", 29)).eval(strict_file)


def test_integer_gt_eq(strict_schema, strict_file):
    assert not StrictMetricsEvaluator(strict_schema, Expressions.greater_than_or_equal("id", 80)).eval(strict_file)
    assert not StrictMetricsEvaluator(strict_schema, Expressions.greater_than_or_equal("id", 79)).eval(strict_file)
    assert not StrictMetricsEvaluator(strict_schema, Expressions.greater_than_or_equal("id", 31)).eval(strict_file)
    assert StrictMetricsEvaluator(strict_schema, Expressions.greater_than_or_equal("id", 30)).eval(strict_file)


def test_integer_eq(strict_schema, strict_file):
    assert not StrictMetricsEvaluator(strict_schema, Expressions.equal("id", 5)).eval(strict_file)
    assert not StrictMetricsEvaluator(strict_schema, Expressions.equal("id", 30)).eval(strict_file)
    assert not StrictMetricsEvaluator(strict_schema, Expressions.equal("id", 75)).eval(strict_file)
    assert not StrictMetricsEvaluator(strict_schema, Expressions.equal("id", 79)).eval(strict_file)
    assert not StrictMetricsEvaluator(strict_schema, Expressions.equal("id", 80)).eval(strict_file)
    assert StrictMetricsEvaluator(strict_schema, Expressions.equal("always_5", 5)).eval(strict_file)


def test_integer_not_eq(strict_schema, strict_file):
    assert StrictMetricsEvaluator(strict_schema, Expressions.not_equal("id", 5)).eval(strict_file)
    assert StrictMetricsEvaluator(strict_schema, Expressions.not_equal("id", 29)).eval(strict_file)
    assert not StrictMetricsEvaluator(strict_schema, Expressions.not_equal("id", 30)).eval(strict_file)
    assert not StrictMetricsEvaluator(strict_schema, Expressions.not_equal("id", 75)).eval(strict_file)
    assert not StrictMetricsEvaluator(strict_schema, Expressions.not_equal("id", 79)).eval(strict_file)
    assert StrictMetricsEvaluator(strict_schema, Expressions.not_equal("id", 80)).eval(strict_file)
    assert StrictMetricsEvaluator(strict_schema, Expressions.not_equal("id", 85)).eval(strict_file)


def test_not_eq_rewritten(strict_schema, strict_file):
    assert StrictMetricsEvaluator(strict_schema, Expressions.not_(Expressions.equal("id", 5))).eval(strict_file)
    assert StrictMetricsEvaluator(strict_schema, Expressions.not_(Expressions.equal("id", 29))).eval(strict_file)
    assert not StrictMetricsEvaluator(strict_schema, Expressions.not_(Expressions.equal("id", 30))).eval(strict_file)
    assert not StrictMetricsEvaluator(strict_schema, Expressions.not_(Expressions.equal("id", 75))).eval(strict_file)
    assert not StrictMetricsEvaluator(strict_schema, Expressions.not_(Expressions.equal("id", 79))).eval(strict_file)
    assert StrictMetricsEvaluator(strict_schema, Expressions.not_(Expressions.equal("id", 80))).eval(strict_file)
    assert StrictMetricsEvaluator(strict_schema, Expressions.not_(Expressions.equal("id", 85))).eval(strict_file)
