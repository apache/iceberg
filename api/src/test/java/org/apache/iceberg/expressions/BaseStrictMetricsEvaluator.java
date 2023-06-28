/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.expressions;

import static org.apache.iceberg.types.Conversions.toByteBuffer;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;

public abstract class BaseStrictMetricsEvaluator {
  protected static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get()),
          optional(2, "no_stats", Types.IntegerType.get()),
          required(3, "required", Types.StringType.get()),
          optional(4, "all_nulls", Types.StringType.get()),
          optional(5, "some_nulls", Types.StringType.get()),
          optional(6, "no_nulls", Types.StringType.get()),
          required(7, "always_5", Types.IntegerType.get()),
          optional(8, "all_nans", Types.DoubleType.get()),
          optional(9, "some_nans", Types.FloatType.get()),
          optional(10, "no_nans", Types.FloatType.get()),
          optional(11, "all_nulls_double", Types.DoubleType.get()),
          optional(12, "all_nans_v1_stats", Types.FloatType.get()),
          optional(13, "nan_and_null_only", Types.DoubleType.get()),
          optional(14, "no_nan_stats", Types.DoubleType.get()));

  protected static final int INT_MIN_VALUE = 30;
  protected static final int INT_MAX_VALUE = 79;

  protected static final DataFile FILE =
      new TestHelpers.TestDataFile(
          "file.avro",
          TestHelpers.Row.of(),
          50,
          // any value counts, including nulls
          ImmutableMap.<Integer, Long>builder()
              .put(4, 50L)
              .put(5, 50L)
              .put(6, 50L)
              .put(8, 50L)
              .put(9, 50L)
              .put(10, 50L)
              .put(11, 50L)
              .put(12, 50L)
              .put(13, 50L)
              .put(14, 50L)
              .buildOrThrow(),
          // null value counts
          ImmutableMap.<Integer, Long>builder()
              .put(4, 50L)
              .put(5, 10L)
              .put(6, 0L)
              .put(11, 50L)
              .put(12, 0L)
              .put(13, 1L)
              .buildOrThrow(),
          // nan value counts
          ImmutableMap.of(
              8, 50L,
              9, 10L,
              10, 0L),
          // lower bounds
          ImmutableMap.of(
              1, toByteBuffer(Types.IntegerType.get(), INT_MIN_VALUE),
              7, toByteBuffer(Types.IntegerType.get(), 5),
              12, toByteBuffer(Types.FloatType.get(), Float.NaN),
              13, toByteBuffer(Types.DoubleType.get(), Double.NaN)),
          // upper bounds
          ImmutableMap.of(
              1, toByteBuffer(Types.IntegerType.get(), INT_MAX_VALUE),
              7, toByteBuffer(Types.IntegerType.get(), 5),
              12, toByteBuffer(Types.FloatType.get(), Float.NaN),
              13, toByteBuffer(Types.DoubleType.get(), Double.NaN)));

  protected static final DataFile FILE_2 =
      new TestHelpers.TestDataFile(
          "file_2.avro",
          TestHelpers.Row.of(),
          50,
          // any value counts, including nulls
          ImmutableMap.of(
              4, 50L,
              5, 50L,
              6, 50L,
              8, 50L),
          // null value counts
          ImmutableMap.of(
              4, 50L,
              5, 10L,
              6, 0L),
          // nan value counts
          null,
          // lower bounds
          ImmutableMap.of(5, toByteBuffer(Types.StringType.get(), "bbb")),
          // upper bounds
          ImmutableMap.of(5, toByteBuffer(Types.StringType.get(), "eee")));

  protected static final DataFile FILE_3 =
      new TestHelpers.TestDataFile(
          "file_3.avro",
          TestHelpers.Row.of(),
          50,
          // any value counts, including nulls
          ImmutableMap.of(
              4, 50L,
              5, 50L,
              6, 50L),
          // null value counts
          ImmutableMap.of(
              4, 50L,
              5, 10L,
              6, 0L),
          // nan value counts
          null,
          // lower bounds
          ImmutableMap.of(5, toByteBuffer(Types.StringType.get(), "bbb")),
          // upper bounds
          ImmutableMap.of(5, toByteBuffer(Types.StringType.get(), "bbb")));
}
