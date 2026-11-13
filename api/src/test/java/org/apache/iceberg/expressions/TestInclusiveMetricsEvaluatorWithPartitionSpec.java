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

import static org.apache.iceberg.expressions.Expressions.and;
import static org.apache.iceberg.expressions.Expressions.in;
import static org.apache.iceberg.expressions.Expressions.notIn;
import static org.apache.iceberg.types.Conversions.toByteBuffer;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Set;
import java.util.function.Function;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.TestHelpers.Row;
import org.apache.iceberg.TestHelpers.TestDataFile;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.StringType;
import org.junit.jupiter.api.Test;

public class TestInclusiveMetricsEvaluatorWithPartitionSpec {
  private static final Schema SCHEMA =
      new Schema(required(1, "id", IntegerType.get()), optional(2, "data", Types.StringType.get()));
  private static final int BUCKETS = 16;
  private static final Function<Integer, Integer> BUCKET =
      Transforms.<Integer>bucket(BUCKETS).bind(IntegerType.get());
  private static final Function<CharSequence, Integer> STRING_BUCKET =
      Transforms.<CharSequence>bucket(BUCKETS).bind(StringType.get());

  /**
   * Id bounds drawn from one bucket, a value from that bucket outside the bounds, and a value from
   * another bucket inside the bounds.
   */
  private record BucketCase(int bucket, int lower, int upper, int ownValue, int foreignValue) {}

  private static BucketCase bucketCase() {
    int bucket = BUCKET.apply(100);
    List<Integer> values = Lists.newArrayList();
    for (int value = 0; values.size() < 7; value += 1) {
      if (BUCKET.apply(value) == bucket) {
        values.add(value);
      }
    }

    int lower = values.get(0);
    int upper = values.get(5);
    int foreignValue = lower + 1;
    while (BUCKET.apply(foreignValue) == bucket) {
      foreignValue += 1;
    }
    assertThat(foreignValue).isLessThan(upper);

    return new BucketCase(bucket, lower, upper, values.get(6), foreignValue);
  }

  private static DataFile fileWithIdBounds(StructLike partition, int lower, int upper) {
    return new TestDataFile(
        "file.parquet",
        partition,
        10,
        ImmutableMap.of(1, 10L),
        ImmutableMap.of(1, 0L),
        null,
        ImmutableMap.of(1, toByteBuffer(IntegerType.get(), lower)),
        ImmutableMap.of(1, toByteBuffer(IntegerType.get(), upper)));
  }

  private static DataFile fileWithDataBounds(StructLike partition, String lower, String upper) {
    return new TestDataFile(
        "file.parquet",
        partition,
        10,
        ImmutableMap.of(2, 10L),
        ImmutableMap.of(2, 0L),
        null,
        ImmutableMap.of(2, toByteBuffer(StringType.get(), lower)),
        ImmutableMap.of(2, toByteBuffer(StringType.get(), upper)));
  }

  private static DataFile fileWithoutBounds(StructLike partition) {
    return new TestDataFile("file.parquet", partition, 10);
  }

  /** Values from the case's bucket above its upper bound, so they cannot match the file. */
  private static Set<Integer> ownValuesAboveBounds(BucketCase bucketCase, int count) {
    Set<Integer> values = Sets.newHashSet();
    for (int value = bucketCase.upper() + 1; values.size() < count; value += 1) {
      if (BUCKET.apply(value) == bucketCase.bucket()) {
        values.add(value);
      }
    }

    return values;
  }

  private static boolean schemaOnly(Expression expr, DataFile file) {
    return new InclusiveMetricsEvaluator(SCHEMA, expr).eval(file);
  }

  private static boolean partitionAware(PartitionSpec spec, Expression expr, DataFile file) {
    return InclusiveMetricsEvaluator.forRowFilter(expr, spec, true).eval(file);
  }

  @Test
  public void testBucketInUsesLiteralsForFilePartition() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).bucket("id", BUCKETS).build();
    BucketCase bucketCase = bucketCase();
    DataFile file =
        fileWithIdBounds(Row.of(bucketCase.bucket()), bucketCase.lower(), bucketCase.upper());

    Expression foreignOnly = in("id", bucketCase.ownValue(), bucketCase.foreignValue());
    assertThat(schemaOnly(foreignOnly, file))
        .as("Schema-only evaluator should read: foreign-bucket literal is inside the bounds")
        .isTrue();
    assertThat(partitionAware(spec, foreignOnly, file))
        .as("Should not read: only literal inside the bounds belongs to another bucket")
        .isFalse();

    Expression withOwnValue = in("id", bucketCase.lower(), bucketCase.foreignValue());
    assertThat(partitionAware(spec, withOwnValue, file))
        .as("Should read: a literal from the file's bucket is inside the bounds")
        .isTrue();
  }

  @Test
  public void testBucketInSkipsFileWithNullPartitionValue() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).bucket("id", BUCKETS).build();
    BucketCase bucketCase = bucketCase();
    DataFile file = fileWithIdBounds(Row.of((Object) null), bucketCase.lower(), bucketCase.upper());

    Expression expr = in("id", bucketCase.lower(), bucketCase.foreignValue());
    assertThat(schemaOnly(expr, file)).isTrue();
    assertThat(partitionAware(spec, expr, file))
        .as("Should not read: a null bucket value cannot match any IN literal")
        .isFalse();
  }

  @Test
  public void testNotInIsUnchanged() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).bucket("id", BUCKETS).build();
    BucketCase bucketCase = bucketCase();
    DataFile rangeFile =
        fileWithIdBounds(Row.of(bucketCase.bucket()), bucketCase.lower(), bucketCase.upper());
    DataFile singleValueFile =
        fileWithIdBounds(Row.of(bucketCase.bucket()), bucketCase.lower(), bucketCase.lower());

    for (DataFile file : List.of(rangeFile, singleValueFile)) {
      for (Expression expr :
          List.of(
              notIn("id", bucketCase.lower(), bucketCase.foreignValue()),
              notIn("id", bucketCase.foreignValue()))) {
        assertThat(partitionAware(spec, expr, file)).isEqualTo(schemaOnly(expr, file));
      }
    }
  }

  @Test
  public void testCaseInsensitiveBinding() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).bucket("id", BUCKETS).build();
    BucketCase bucketCase = bucketCase();
    DataFile file =
        fileWithIdBounds(Row.of(bucketCase.bucket()), bucketCase.lower(), bucketCase.upper());
    Expression expr = in("ID", bucketCase.ownValue(), bucketCase.foreignValue());

    assertThat(InclusiveMetricsEvaluator.forRowFilter(expr, spec, false).eval(file)).isFalse();
    assertThatThrownBy(() -> InclusiveMetricsEvaluator.forRowFilter(expr, spec, true))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Cannot find field 'ID'");
  }

  @Test
  public void testUnpartitionedSpecMatchesSchemaOnlyEvaluator() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).build();
    BucketCase bucketCase = bucketCase();
    DataFile file = fileWithIdBounds(Row.of(), bucketCase.lower(), bucketCase.upper());

    for (Expression expr :
        List.of(
            in("id", bucketCase.ownValue(), bucketCase.foreignValue()),
            in("id", bucketCase.ownValue()),
            notIn("id", bucketCase.foreignValue()))) {
      assertThat(partitionAware(spec, expr, file)).isEqualTo(schemaOnly(expr, file));
    }
  }

  @Test
  public void testOrderPreservingTransformsUseAllLiterals() {
    BucketCase bucketCase = bucketCase();
    Expression expr = in("id", bucketCase.ownValue(), bucketCase.foreignValue());

    PartitionSpec identity = PartitionSpec.builderFor(SCHEMA).identity("id").build();
    DataFile identityFile =
        fileWithIdBounds(
            Row.of(bucketCase.foreignValue()),
            bucketCase.foreignValue(),
            bucketCase.foreignValue());
    assertThat(partitionAware(identity, expr, identityFile))
        .isEqualTo(schemaOnly(expr, identityFile))
        .isTrue();

    PartitionSpec truncate = PartitionSpec.builderFor(SCHEMA).truncate("id", 1000).build();
    DataFile truncateFile = fileWithIdBounds(Row.of(0), bucketCase.lower(), bucketCase.upper());
    assertThat(partitionAware(truncate, expr, truncateFile))
        .as("Should read: truncate is order preserving, so all literals are compared")
        .isEqualTo(schemaOnly(expr, truncateFile))
        .isTrue();
  }

  @Test
  public void testBucketWithOrderPreservingFieldOnSameColumn() {
    PartitionSpec spec =
        PartitionSpec.builderFor(SCHEMA).truncate("id", 1000).bucket("id", BUCKETS).build();
    BucketCase bucketCase = bucketCase();
    assertThat(bucketCase.upper()).isLessThan(1000);
    DataFile file =
        fileWithIdBounds(Row.of(0, bucketCase.bucket()), bucketCase.lower(), bucketCase.upper());

    Expression foreignOnly = in("id", bucketCase.ownValue(), bucketCase.foreignValue());
    assertThat(schemaOnly(foreignOnly, file)).isTrue();
    assertThat(partitionAware(spec, foreignOnly, file))
        .as("Should not read: the bucket field narrows literals even with a truncate field")
        .isFalse();

    Expression withOwnValue = in("id", bucketCase.lower(), bucketCase.foreignValue());
    assertThat(partitionAware(spec, withOwnValue, file)).isTrue();
  }

  @Test
  public void testBucketInWithinAnd() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).bucket("id", BUCKETS).build();
    BucketCase bucketCase = bucketCase();
    DataFile file =
        fileWithIdBounds(Row.of(bucketCase.bucket()), bucketCase.lower(), bucketCase.upper());

    Expression expr =
        and(
            in("id", bucketCase.ownValue(), bucketCase.foreignValue()),
            notIn("id", bucketCase.lower()));
    assertThat(schemaOnly(expr, file)).isTrue();
    assertThat(partitionAware(spec, expr, file)).isFalse();
  }

  @Test
  public void testBucketInSkipsFileWithoutBoundsWhenNoLiteralMatchesBucket() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).bucket("id", BUCKETS).build();
    BucketCase bucketCase = bucketCase();
    DataFile file = fileWithoutBounds(Row.of(bucketCase.bucket()));
    int otherForeignValue = bucketCase.foreignValue() + 1;
    while (BUCKET.apply(otherForeignValue) == bucketCase.bucket()) {
      otherForeignValue += 1;
    }

    // two literals, because binding rewrites a single-literal IN as an equality
    Expression foreignOnly = in("id", bucketCase.foreignValue(), otherForeignValue);
    assertThat(schemaOnly(foreignOnly, file)).isTrue();
    assertThat(partitionAware(spec, foreignOnly, file))
        .as("Should not read: no literal maps to the file's bucket, so bounds are not needed")
        .isFalse();

    Expression withOwnValue = in("id", bucketCase.ownValue(), bucketCase.foreignValue());
    assertThat(partitionAware(spec, withOwnValue, file))
        .as("Should read: a literal maps to the file's bucket and the file has no bounds")
        .isTrue();

    DataFile nullBucketFile = fileWithoutBounds(Row.of((Object) null));
    assertThat(partitionAware(spec, withOwnValue, nullBucketFile))
        .as("Should not read: a null bucket value cannot match any IN literal")
        .isFalse();
  }

  @Test
  public void testBucketInNarrowsBeforeInPredicateLimit() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).bucket("id", BUCKETS).build();
    BucketCase bucketCase = bucketCase();
    DataFile file =
        fileWithIdBounds(Row.of(bucketCase.bucket()), bucketCase.lower(), bucketCase.upper());

    Set<Integer> narrowedTo200 = ownValuesAboveBounds(bucketCase, 200);
    narrowedTo200.add(bucketCase.foreignValue());
    Expression atLimit = in("id", narrowedTo200);
    assertThat(schemaOnly(atLimit, file))
        .as("Schema-only evaluator should read: 201 literals exceed the limit")
        .isTrue();
    assertThat(partitionAware(spec, atLimit, file))
        .as("Should not read: 200 literals remain after narrowing and none is inside the bounds")
        .isFalse();

    Set<Integer> narrowedTo201 = ownValuesAboveBounds(bucketCase, 201);
    narrowedTo201.add(bucketCase.foreignValue());
    assertThat(partitionAware(spec, in("id", narrowedTo201), file))
        .as("Should read: 201 literals remain after narrowing, which exceeds the limit")
        .isTrue();

    Set<Integer> withOwnValueInBounds = ownValuesAboveBounds(bucketCase, 199);
    withOwnValueInBounds.add(bucketCase.lower());
    withOwnValueInBounds.add(bucketCase.foreignValue());
    assertThat(partitionAware(spec, in("id", withOwnValueInBounds), file))
        .as("Should read: a literal from the file's bucket is inside the bounds")
        .isTrue();
  }

  @Test
  public void testBucketInOnStringColumn() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).bucket("data", BUCKETS).build();
    String ownValue = "key-0";
    int bucket = STRING_BUCKET.apply(ownValue);
    List<String> foreignValues = Lists.newArrayList();
    for (int index = 1; foreignValues.size() < 2; index += 1) {
      String candidate = "key-" + index;
      if (STRING_BUCKET.apply(candidate) != bucket) {
        foreignValues.add(candidate);
      }
    }

    DataFile file = fileWithDataBounds(Row.of(bucket), "key-", "key-~");

    Expression foreignOnly = in("data", foreignValues.get(0), foreignValues.get(1));
    assertThat(schemaOnly(foreignOnly, file)).isTrue();
    assertThat(partitionAware(spec, foreignOnly, file))
        .as("Should not read: the only literal inside the bounds belongs to another bucket")
        .isFalse();

    assertThat(partitionAware(spec, in("data", ownValue, foreignValues.get(0)), file))
        .as("Should read: a literal from the file's bucket is inside the bounds")
        .isTrue();
  }
}
