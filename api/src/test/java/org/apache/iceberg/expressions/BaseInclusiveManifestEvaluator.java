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

import java.nio.ByteBuffer;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types;

public abstract class BaseInclusiveManifestEvaluator {
  protected static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get()),
          optional(4, "all_nulls_missing_nan", Types.StringType.get()),
          optional(5, "some_nulls", Types.StringType.get()),
          optional(6, "no_nulls", Types.StringType.get()),
          optional(7, "float", Types.FloatType.get()),
          optional(8, "all_nulls_double", Types.DoubleType.get()),
          optional(9, "all_nulls_no_nans", Types.FloatType.get()),
          optional(10, "all_nans", Types.DoubleType.get()),
          optional(11, "both_nan_and_null", Types.FloatType.get()),
          optional(12, "no_nan_or_null", Types.DoubleType.get()),
          optional(13, "all_nulls_missing_nan_float", Types.FloatType.get()),
          optional(14, "all_same_value_or_null", Types.StringType.get()),
          optional(15, "no_nulls_same_value_a", Types.StringType.get()));

  protected static final PartitionSpec SPEC =
      PartitionSpec.builderFor(SCHEMA)
          .withSpecId(0)
          .identity("id")
          .identity("all_nulls_missing_nan")
          .identity("some_nulls")
          .identity("no_nulls")
          .identity("float")
          .identity("all_nulls_double")
          .identity("all_nulls_no_nans")
          .identity("all_nans")
          .identity("both_nan_and_null")
          .identity("no_nan_or_null")
          .identity("all_nulls_missing_nan_float")
          .identity("all_same_value_or_null")
          .identity("no_nulls_same_value_a")
          .build();

  protected static final int INT_MIN_VALUE = 30;
  protected static final int INT_MAX_VALUE = 79;

  protected static final ByteBuffer INT_MIN = toByteBuffer(Types.IntegerType.get(), INT_MIN_VALUE);
  protected static final ByteBuffer INT_MAX = toByteBuffer(Types.IntegerType.get(), INT_MAX_VALUE);

  protected static final ByteBuffer STRING_MIN = toByteBuffer(Types.StringType.get(), "a");
  protected static final ByteBuffer STRING_MAX = toByteBuffer(Types.StringType.get(), "z");

  protected static final ManifestFile NO_STATS =
      new TestHelpers.TestManifestFile(
          "manifest-list.avro", 1024, 0, System.currentTimeMillis(), null, null, null, null, null);

  protected static final ManifestFile FILE =
      new TestHelpers.TestManifestFile(
          "manifest-list.avro",
          1024,
          0,
          System.currentTimeMillis(),
          5,
          10,
          0,
          ImmutableList.of(
              new TestHelpers.TestFieldSummary(false, INT_MIN, INT_MAX),
              new TestHelpers.TestFieldSummary(true, null, null),
              new TestHelpers.TestFieldSummary(true, STRING_MIN, STRING_MAX),
              new TestHelpers.TestFieldSummary(false, STRING_MIN, STRING_MAX),
              new TestHelpers.TestFieldSummary(
                  false,
                  toByteBuffer(Types.FloatType.get(), 0F),
                  toByteBuffer(Types.FloatType.get(), 20F)),
              new TestHelpers.TestFieldSummary(true, null, null),
              new TestHelpers.TestFieldSummary(true, false, null, null),
              new TestHelpers.TestFieldSummary(false, true, null, null),
              new TestHelpers.TestFieldSummary(true, true, null, null),
              new TestHelpers.TestFieldSummary(
                  false,
                  false,
                  toByteBuffer(Types.FloatType.get(), 0F),
                  toByteBuffer(Types.FloatType.get(), 20F)),
              new TestHelpers.TestFieldSummary(true, null, null),
              new TestHelpers.TestFieldSummary(true, STRING_MIN, STRING_MIN),
              new TestHelpers.TestFieldSummary(false, STRING_MIN, STRING_MIN)),
          null);
}
