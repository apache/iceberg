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
                                     InclusiveMetricsEvaluator)
from iceberg.exceptions import ValidationException
from pytest import raises


def test_all_nulls(schema, file):
    # Should skip: no non-null value in all null column
    assert not InclusiveMetricsEvaluator(schema, Expressions.not_null("all_nulls")).eval(file)
    # Should read: column with some nulls contains a non-null value
    assert InclusiveMetricsEvaluator(schema, Expressions.not_null("some_nulls")).eval(file)
    # Should read: non-null column contains a non-null value
    assert InclusiveMetricsEvaluator(schema, Expressions.not_null("no_nulls")).eval(file)


def test_no_nulls(schema, file):
    # Should read: at least one null value in all null column
    assert InclusiveMetricsEvaluator(schema, Expressions.is_null("all_nulls")).eval(file)
    # Should read: column with some nulls contains a null value
    assert InclusiveMetricsEvaluator(schema, Expressions.is_null("some_nulls")).eval(file)
    # Should skip: non-null column contains no null values
    assert not InclusiveMetricsEvaluator(schema, Expressions.is_null("no_nulls")).eval(file)


def test_required_column(schema, file):
    assert InclusiveMetricsEvaluator(schema, Expressions.not_null("required")).eval(file)
    assert not InclusiveMetricsEvaluator(schema, Expressions.is_null("required")).eval(file)


def test_missing_column(schema, file):
    with raises(RuntimeError):
        InclusiveMetricsEvaluator(schema, Expressions.less_than("missing", 5)).eval(file)


def test_missing_stats(schema, missing_stats, missing_stats_exprs):
    assert InclusiveMetricsEvaluator(schema, missing_stats_exprs).eval(missing_stats)


def test_zero_record_file(schema, empty, zero_rows_exprs):
    assert not InclusiveMetricsEvaluator(schema, zero_rows_exprs).eval(empty)


def test_not(schema, file):
    assert InclusiveMetricsEvaluator(schema, Expressions.not_(Expressions.less_than("id", 5))).eval(file)
    assert not InclusiveMetricsEvaluator(schema,
                                         Expressions.not_(Expressions.greater_than("id", 5))).eval(file)


def test_and(schema, file):
    assert not InclusiveMetricsEvaluator(schema,
                                         Expressions.and_(Expressions.less_than("id", 5),
                                                          Expressions.greater_than_or_equal("id", 0))).eval(file)
    assert InclusiveMetricsEvaluator(schema, Expressions.and_(Expressions.greater_than("id", 5),
                                                              Expressions.less_than_or_equal("id", 30))).eval(file)


def test_or(schema, file):
    assert not InclusiveMetricsEvaluator(schema,
                                         Expressions.or_(Expressions.less_than("id", 5),
                                                         Expressions.greater_than_or_equal("id", 80))).eval(file)
    assert InclusiveMetricsEvaluator(schema,
                                     Expressions.or_(Expressions.less_than("id", 5),
                                                     Expressions.greater_than_or_equal("id", 60))).eval(file)


def test_integer_lt(schema, file):
    assert not InclusiveMetricsEvaluator(schema, Expressions.less_than("id", 5)).eval(file)
    assert not InclusiveMetricsEvaluator(schema, Expressions.less_than("id", 30)).eval(file)
    assert InclusiveMetricsEvaluator(schema, Expressions.less_than("id", 31)).eval(file)
    assert InclusiveMetricsEvaluator(schema, Expressions.less_than("id", 79)).eval(file)


def test_integer_gt(schema, file):
    assert not InclusiveMetricsEvaluator(schema, Expressions.greater_than("id", 85)).eval(file)
    assert not InclusiveMetricsEvaluator(schema, Expressions.greater_than("id", 79)).eval(file)
    assert InclusiveMetricsEvaluator(schema, Expressions.greater_than("id", 78)).eval(file)
    assert InclusiveMetricsEvaluator(schema, Expressions.greater_than("id", 75)).eval(file)


def test_integer_gt_eq(schema, file):
    assert not InclusiveMetricsEvaluator(schema, Expressions.greater_than_or_equal("id", 85)).eval(file)
    assert not InclusiveMetricsEvaluator(schema, Expressions.greater_than_or_equal("id", 80)).eval(file)
    assert InclusiveMetricsEvaluator(schema, Expressions.greater_than_or_equal("id", 79)).eval(file)
    assert InclusiveMetricsEvaluator(schema, Expressions.greater_than_or_equal("id", 75)).eval(file)


def test_integer_eq(schema, file):
    assert not InclusiveMetricsEvaluator(schema, Expressions.equal("id", 5)).eval(file)
    assert not InclusiveMetricsEvaluator(schema, Expressions.equal("id", 29)).eval(file)
    assert not InclusiveMetricsEvaluator(schema, Expressions.equal("id", 80)).eval(file)
    assert not InclusiveMetricsEvaluator(schema, Expressions.equal("id", 85)).eval(file)
    assert InclusiveMetricsEvaluator(schema, Expressions.equal("id", 30)).eval(file)
    assert InclusiveMetricsEvaluator(schema, Expressions.equal("id", 75)).eval(file)
    assert InclusiveMetricsEvaluator(schema, Expressions.equal("id", 79)).eval(file)


def test_integer_not_eq(schema, file, not_eq):
    assert InclusiveMetricsEvaluator(schema, not_eq).eval(file)


def test_not_eq_rewritten(schema, file, not_eq_rewrite):
    assert InclusiveMetricsEvaluator(schema, Expressions.not_(not_eq_rewrite)).eval(file)


def test_case_insensitive_int_not_eq_rewritten(schema, file, not_eq_uc):
    assert InclusiveMetricsEvaluator(schema, Expressions.not_(not_eq_uc),
                                     case_sensitive=False).eval(file)


def test_case_sensitive_int_not_eq_rewritten(schema, file, not_eq_uc):
    with raises(ValidationException):
        assert InclusiveMetricsEvaluator(schema, Expressions.not_(not_eq_uc),
                                         case_sensitive=True).eval(file)
