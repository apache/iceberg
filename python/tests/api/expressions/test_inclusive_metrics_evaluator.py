# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# flake8: noqa
import unittest

from iceberg.api.expressions import (InclusiveMetricsEvaluator,
                                     Expressions)
from iceberg.api.schema import Schema
from iceberg.api.types import (Conversions,
                               IntegerType,
                               NestedField,
                               StringType)
from nose.tools import raises

from tests.api.test_helpers import (TestDataFile,
                                    TestHelpers)

SCHEMA = Schema(NestedField.required(1, "id", IntegerType.get()),
                NestedField.optional(2, "no_stats", IntegerType.get()),
                NestedField.required(3, "required", StringType.get()),
                NestedField.optional(4, "all_nulls", StringType.get()),
                NestedField.optional(5, "some_nulls", StringType.get()),
                NestedField.optional(6, "no_nulls", StringType.get()))

FILE = TestDataFile("file.avro", TestHelpers.Row.of(), 50,
                    # value counts
                    {4:50,
                     5:50,
                     6:50},
                    # null value counts
                    {4: 50,
                     5: 10,
                     6: 0},
                    # lower bounds
                    {1: Conversions.to_byte_buffer(IntegerType.get(), 30)},
                    # upper bounds
                    {1: Conversions.to_byte_buffer(IntegerType.get(), 79)})


class TestInclusiveMetricsEvaluator(unittest.TestCase):

    def test_all_nulls(self):
        # Should skip: no non-null value in all null column
        self.assertFalse(InclusiveMetricsEvaluator(SCHEMA, Expressions.not_null("all_nulls")).eval(FILE))
        # Should read: column with some nulls contains a non-null value
        self.assertTrue(InclusiveMetricsEvaluator(SCHEMA, Expressions.not_null("some_nulls")).eval(FILE))
        # Should read: non-null column contains a non-null value
        self.assertTrue(InclusiveMetricsEvaluator(SCHEMA, Expressions.not_null("no_nulls")).eval(FILE))

    def test_no_nulls(self):
        # Should read: at least one null value in all null column
        self.assertTrue(InclusiveMetricsEvaluator(SCHEMA, Expressions.is_null("all_nulls")).eval(FILE))
        # Should read: column with some nulls contains a null value
        self.assertTrue(InclusiveMetricsEvaluator(SCHEMA, Expressions.is_null("some_nulls")).eval(FILE))
        # Should skip: non-null column contains no null values
        self.assertFalse(InclusiveMetricsEvaluator(SCHEMA, Expressions.is_null("no_nulls")).eval(FILE))

    def test_required_column(self):
        self.assertTrue(InclusiveMetricsEvaluator(SCHEMA, Expressions.not_null("required")).eval(FILE))
        self.assertFalse(InclusiveMetricsEvaluator(SCHEMA, Expressions.is_null("required")).eval(FILE))

    @raises(RuntimeError)
    def test_missing_column(self):
        InclusiveMetricsEvaluator(SCHEMA, Expressions.less_than("missing", 5)).eval(FILE)

    def test_missing_stats(self):
        missing_stats = TestDataFile("file.parquet", TestHelpers.Row.of(), 50)
        exprs = [Expressions.less_than("no_stats", 5),
                 Expressions.less_than_or_equal("no_stats", 30),
                 Expressions.equal("no_stats", 70),
                 Expressions.greater_than("no_stats", 78),
                 Expressions.greater_than_or_equal("no_stats", 90),
                 Expressions.not_equal("no_stats", 101),
                 Expressions.is_null("no_stats"),
                 Expressions.not_null("no_stats")]
        for expr in exprs:
            should_read = InclusiveMetricsEvaluator(SCHEMA, expr).eval(missing_stats)
            self.assertTrue(should_read)

    def test_zero_record_file(self):
        empty = TestDataFile("file.parquet", TestHelpers.Row.of(), record_count=0)
        exprs = [Expressions.less_than("id", 5),
                 Expressions.less_than_or_equal("id", 30),
                 Expressions.equal("id", 70),
                 Expressions.greater_than("id", 78),
                 Expressions.greater_than_or_equal("id", 90),
                 Expressions.not_equal("id", 101),
                 Expressions.is_null("some_nulls"),
                 Expressions.not_null("some_nulls")]

        for expr in exprs:
            self.assertFalse(InclusiveMetricsEvaluator(SCHEMA, expr).eval(empty))

    def test_not(self):
        self.assertTrue(InclusiveMetricsEvaluator(SCHEMA,
                                                  Expressions.not_(Expressions.less_than("id",
                                                                                         5))).eval(FILE))
        self.assertFalse(InclusiveMetricsEvaluator(SCHEMA,
                                                   Expressions.not_(Expressions.greater_than("id",
                                                                                             5))).eval(FILE))

    def test_and(self):
        self.assertFalse(InclusiveMetricsEvaluator(SCHEMA,
                                                   Expressions.and_(Expressions.less_than("id",
                                                                                          5),
                                                                    Expressions.greater_than_or_equal("id",
                                                                                                      0))).eval(FILE))
        self.assertTrue(InclusiveMetricsEvaluator(SCHEMA,
                                                   Expressions.and_(Expressions.greater_than("id",
                                                                                             5),
                                                                    Expressions.less_than_or_equal("id",
                                                                                                   30))).eval(FILE))

    def test_or(self):
        self.assertFalse(InclusiveMetricsEvaluator(SCHEMA,
                                                   Expressions.or_(Expressions.less_than("id",
                                                                                          5),
                                                                    Expressions.greater_than_or_equal("id",
                                                                                                      80))).eval(FILE))
        self.assertTrue(InclusiveMetricsEvaluator(SCHEMA,
                                                  Expressions.or_(Expressions.less_than("id",
                                                                                        5),
                                                                   Expressions.greater_than_or_equal("id",
                                                                                                     60))).eval(FILE))

    def test_integer_lt(self):
        self.assertFalse(InclusiveMetricsEvaluator(SCHEMA, Expressions.less_than("id", 5)).eval(FILE))
        self.assertFalse(InclusiveMetricsEvaluator(SCHEMA, Expressions.less_than("id", 30)).eval(FILE))
        self.assertTrue(InclusiveMetricsEvaluator(SCHEMA, Expressions.less_than("id", 31)).eval(FILE))
        self.assertTrue(InclusiveMetricsEvaluator(SCHEMA, Expressions.less_than("id", 79)).eval(FILE))

    def test_integer_gt(self):
        self.assertFalse(InclusiveMetricsEvaluator(SCHEMA, Expressions.greater_than("id", 85)).eval(FILE))
        self.assertFalse(InclusiveMetricsEvaluator(SCHEMA, Expressions.greater_than("id", 79)).eval(FILE))
        self.assertTrue(InclusiveMetricsEvaluator(SCHEMA, Expressions.greater_than("id", 78)).eval(FILE))
        self.assertTrue(InclusiveMetricsEvaluator(SCHEMA, Expressions.greater_than("id", 75)).eval(FILE))

    def test_integer_gt_eq(self):
        self.assertFalse(InclusiveMetricsEvaluator(SCHEMA, Expressions.greater_than_or_equal("id", 85)).eval(FILE))
        self.assertFalse(InclusiveMetricsEvaluator(SCHEMA, Expressions.greater_than_or_equal("id", 80)).eval(FILE))
        self.assertTrue(InclusiveMetricsEvaluator(SCHEMA, Expressions.greater_than_or_equal("id", 79)).eval(FILE))
        self.assertTrue(InclusiveMetricsEvaluator(SCHEMA, Expressions.greater_than_or_equal("id", 75)).eval(FILE))

    def test_integer_eq(self):
        self.assertFalse(InclusiveMetricsEvaluator(SCHEMA, Expressions.equal("id", 5)).eval(FILE))
        self.assertFalse(InclusiveMetricsEvaluator(SCHEMA, Expressions.equal("id", 29)).eval(FILE))
        self.assertTrue(InclusiveMetricsEvaluator(SCHEMA, Expressions.equal("id", 30)).eval(FILE))
        self.assertTrue(InclusiveMetricsEvaluator(SCHEMA, Expressions.equal("id", 75)).eval(FILE))
        self.assertTrue(InclusiveMetricsEvaluator(SCHEMA, Expressions.equal("id", 79)).eval(FILE))
        self.assertFalse(InclusiveMetricsEvaluator(SCHEMA, Expressions.equal("id", 80)).eval(FILE))
        self.assertFalse(InclusiveMetricsEvaluator(SCHEMA, Expressions.equal("id", 85)).eval(FILE))

    def test_integer_not_eq(self):
        self.assertTrue(InclusiveMetricsEvaluator(SCHEMA, Expressions.not_equal("id", 5)).eval(FILE))
        self.assertTrue(InclusiveMetricsEvaluator(SCHEMA, Expressions.not_equal("id", 29)).eval(FILE))
        self.assertTrue(InclusiveMetricsEvaluator(SCHEMA, Expressions.not_equal("id", 30)).eval(FILE))
        self.assertTrue(InclusiveMetricsEvaluator(SCHEMA, Expressions.not_equal("id", 75)).eval(FILE))
        self.assertTrue(InclusiveMetricsEvaluator(SCHEMA, Expressions.not_equal("id", 79)).eval(FILE))
        self.assertTrue(InclusiveMetricsEvaluator(SCHEMA, Expressions.not_equal("id", 80)).eval(FILE))
        self.assertTrue(InclusiveMetricsEvaluator(SCHEMA, Expressions.not_equal("id", 85)).eval(FILE))

    def test_not_eq_rewritten(self):
        self.assertTrue(InclusiveMetricsEvaluator(SCHEMA, Expressions.not_(Expressions.equal("id", 5))).eval(FILE))
        self.assertTrue(InclusiveMetricsEvaluator(SCHEMA, Expressions.not_(Expressions.equal("id", 29))).eval(FILE))
        self.assertTrue(InclusiveMetricsEvaluator(SCHEMA, Expressions.not_(Expressions.equal("id", 30))).eval(FILE))
        self.assertTrue(InclusiveMetricsEvaluator(SCHEMA, Expressions.not_(Expressions.equal("id", 75))).eval(FILE))
        self.assertTrue(InclusiveMetricsEvaluator(SCHEMA, Expressions.not_(Expressions.equal("id", 79))).eval(FILE))
        self.assertTrue(InclusiveMetricsEvaluator(SCHEMA, Expressions.not_(Expressions.equal("id", 80))).eval(FILE))
        self.assertTrue(InclusiveMetricsEvaluator(SCHEMA, Expressions.not_(Expressions.equal("id", 85))).eval(FILE))

