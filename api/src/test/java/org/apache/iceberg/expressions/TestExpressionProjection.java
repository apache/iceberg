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

import java.nio.ByteBuffer;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.iceberg.expressions.Expressions.day;
import static org.apache.iceberg.expressions.Expressions.equal;
import static org.apache.iceberg.expressions.Expressions.month;
import static org.apache.iceberg.expressions.Expressions.year;
import static org.apache.iceberg.types.Conversions.toByteBuffer;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

public class TestExpressionProjection {

  private static final Schema SCHEMA = new Schema(
      required(1, "timestamp1", Types.TimestampType.withoutZone()),
      optional(2, "timestamp2", Types.TimestampType.withoutZone()),
      optional(3, "timestamp3", Types.TimestampType.withoutZone()),
      optional(4, "date1", Types.DateType.get()),
      optional(5, "date2", Types.DateType.get()),
      optional(6, "date3", Types.DateType.get())
  );

  private static final PartitionSpec SPEC = PartitionSpec.builderFor(SCHEMA)
      .withSpecId(0)
      .day("timestamp1")
      .month("timestamp2")
      .year("timestamp3")
      .day("date1")
      .month("date2")
      .year("date3")
      .build();

  private static final ByteBuffer YEAR_MIN = toByteBuffer(Types.IntegerType.get(), 1);
  private static final ByteBuffer YEAR_MAX = toByteBuffer(Types.IntegerType.get(), 10);

  private static final ByteBuffer MONTH_MIN = toByteBuffer(Types.IntegerType.get(), 1);
  private static final ByteBuffer MONTH_MAX = toByteBuffer(Types.IntegerType.get(), 100);

  private static final ByteBuffer DAY_MIN = toByteBuffer(Types.IntegerType.get(), 1);
  private static final ByteBuffer DAY_MAX = toByteBuffer(Types.IntegerType.get(), 1000);

  private static final ManifestFile FILE = new TestHelpers.TestManifestFile("manifest-list.avro",
      1024, 0, System.currentTimeMillis(), 5, 10, 0, ImmutableList.of(
      new TestHelpers.TestFieldSummary(false, DAY_MIN, DAY_MAX),
      new TestHelpers.TestFieldSummary(false, MONTH_MIN, MONTH_MAX),
      new TestHelpers.TestFieldSummary(false, YEAR_MIN, YEAR_MAX),
      new TestHelpers.TestFieldSummary(false, DAY_MIN, DAY_MAX),
      new TestHelpers.TestFieldSummary(false, MONTH_MIN, MONTH_MAX),
      new TestHelpers.TestFieldSummary(false, YEAR_MIN, YEAR_MAX)
  ));

  @Test
  public void testPartitionFilterOnTimestamp() {
    boolean shouldRead = ManifestEvaluator.forRowFilter(equal(day("timestamp1"), 20), SPEC, true).eval(FILE);
    Assert.assertTrue("Should read: day is in range [1 - 1000]", shouldRead);

    shouldRead = ManifestEvaluator.forRowFilter(equal(day("timestamp1"), 1001), SPEC, true).eval(FILE);
    Assert.assertFalse("Should skip: day is out of range[1 - 1000]", shouldRead);

    shouldRead = ManifestEvaluator.forRowFilter(equal(month("timestamp2"), 20), SPEC, true).eval(FILE);
    Assert.assertTrue("Should read: month is in range [1 - 100]", shouldRead);

    shouldRead = ManifestEvaluator.forRowFilter(equal(month("timestamp2"), 101), SPEC, true).eval(FILE);
    Assert.assertFalse("Should skip: month is out of range[1 - 100]", shouldRead);

    shouldRead = ManifestEvaluator.forRowFilter(equal(year("timestamp3"), 2), SPEC, true).eval(FILE);
    Assert.assertTrue("Should read: year is in range [1 - 10]", shouldRead);

    shouldRead = ManifestEvaluator.forRowFilter(equal(year("timestamp3"), 11), SPEC, true).eval(FILE);
    Assert.assertFalse("Should skip: year is out of range[1 - 10]", shouldRead);
  }

  @Test
  public void testPartitionFilterOnDate() {
    boolean shouldRead = ManifestEvaluator.forRowFilter(equal(day("date1"), 20), SPEC, true).eval(FILE);
    Assert.assertTrue("Should read: day is in range [1 - 1000]", shouldRead);

    shouldRead = ManifestEvaluator.forRowFilter(equal(day("date1"), 1001), SPEC, true).eval(FILE);
    Assert.assertFalse("Should skip: day is out of range[1 - 1000]", shouldRead);

    shouldRead = ManifestEvaluator.forRowFilter(equal(month("date2"), 20), SPEC, true).eval(FILE);
    Assert.assertTrue("Should read: month is in range [1 - 100]", shouldRead);

    shouldRead = ManifestEvaluator.forRowFilter(equal(month("date2"), 101), SPEC, true).eval(FILE);
    Assert.assertFalse("Should skip: month is out of range[1 - 100]", shouldRead);

    shouldRead = ManifestEvaluator.forRowFilter(equal(year("date3"), 2), SPEC, true).eval(FILE);
    Assert.assertTrue("Should read: year is in range [1 - 10]", shouldRead);

    shouldRead = ManifestEvaluator.forRowFilter(equal(year("date3"), 11), SPEC, true).eval(FILE);
    Assert.assertFalse("Should skip: year is out of range[1 - 10]", shouldRead);
  }
}
