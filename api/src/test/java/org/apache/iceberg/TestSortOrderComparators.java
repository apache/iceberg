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
package org.apache.iceberg;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Comparator;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SerializableFunction;
import org.junit.jupiter.api.Test;

public class TestSortOrderComparators {

  private static void assertComparesCorrectly(
      Schema schema,
      SortOrder sortOrder,
      StructLike less,
      StructLike greater,
      StructLike lessCopy,
      StructLike nullValue) {

    Comparator<StructLike> comparator = SortOrderComparators.forSchema(schema, sortOrder);
    // all fields should have the same sort direction in this test class
    assertThat(sortOrder.fields().stream().map(SortField::direction).distinct()).hasSize(1);
    SortDirection direction = sortOrder.fields().get(0).direction();

    assertThat(comparator.compare(less, less)).isEqualTo(0);
    assertThat(comparator.compare(greater, greater)).isEqualTo(0);
    assertThat(comparator.compare(less, lessCopy)).isEqualTo(0);

    if (direction == SortDirection.ASC) {
      assertThat(comparator.compare(less, greater)).isEqualTo(-1);
      assertThat(comparator.compare(greater, less)).isEqualTo(1);
      // null first
      assertThat(comparator.compare(nullValue, less)).isEqualTo(-1);
      assertThat(comparator.compare(less, nullValue)).isEqualTo(1);
    } else {
      assertThat(comparator.compare(less, greater)).isEqualTo(1);
      assertThat(comparator.compare(greater, less)).isEqualTo(-1);
      // null last
      assertThat(comparator.compare(nullValue, greater)).isEqualTo(1);
      assertThat(comparator.compare(less, nullValue)).isEqualTo(-1);
    }
  }

  private static Schema singleSortFildSchema(Type type) {
    return new Schema(
        Types.NestedField.optional(1, "id", Types.StringType.get()),
        Types.NestedField.optional(2, "field", type));
  }

  private static final TestHelpers.Row SINGLE_SORT_FILED_NULL_VALUE =
      TestHelpers.Row.of("id4", null);

  private static SortOrder ascOrder(Schema schema) {
    return SortOrder.builderFor(schema).asc("field").build();
  }

  private static SortOrder descOrder(Schema schema) {
    return SortOrder.builderFor(schema).desc("field", NullOrder.NULLS_LAST).build();
  }

  private static void testIdentitySortField(
      Schema schema, TestHelpers.Row less, TestHelpers.Row greater, TestHelpers.Row lessCopy) {
    assertComparesCorrectly(
        schema, ascOrder(schema), less, greater, lessCopy, SINGLE_SORT_FILED_NULL_VALUE);
    assertComparesCorrectly(
        schema, descOrder(schema), less, greater, lessCopy, SINGLE_SORT_FILED_NULL_VALUE);
  }

  @Test
  public void testBoolean() {
    testIdentitySortField(
        singleSortFildSchema(Types.BooleanType.get()),
        TestHelpers.Row.of("id3", Boolean.FALSE),
        TestHelpers.Row.of("id2", Boolean.TRUE),
        TestHelpers.Row.of("id1", Boolean.FALSE));
  }

  @Test
  public void testInt() {
    testIdentitySortField(
        singleSortFildSchema(Types.IntegerType.get()),
        TestHelpers.Row.of("id3", 111),
        TestHelpers.Row.of("id2", 222),
        TestHelpers.Row.of("id1", 111));
  }

  @Test
  public void testLong() {
    testIdentitySortField(
        singleSortFildSchema(Types.LongType.get()),
        TestHelpers.Row.of("id3", 111L),
        TestHelpers.Row.of("id2", 222L),
        TestHelpers.Row.of("id1", 111L));
  }

  @Test
  public void testFloat() {
    testIdentitySortField(
        singleSortFildSchema(Types.FloatType.get()),
        TestHelpers.Row.of("id3", 1.11f),
        TestHelpers.Row.of("id1", 2.22f),
        TestHelpers.Row.of("id1", 1.11f));
  }

  @Test
  public void testDouble() {
    testIdentitySortField(
        singleSortFildSchema(Types.DoubleType.get()),
        TestHelpers.Row.of("id3", 1.11d),
        TestHelpers.Row.of("id2", 2.22d),
        TestHelpers.Row.of("id1", 1.11d));
  }

  @Test
  public void testDate() {
    testIdentitySortField(
        singleSortFildSchema(Types.DateType.get()),
        TestHelpers.Row.of("id3", 111),
        TestHelpers.Row.of("id2", 222),
        TestHelpers.Row.of("id1", 111));
  }

  @Test
  public void testTime() {
    testIdentitySortField(
        singleSortFildSchema(Types.TimeType.get()),
        TestHelpers.Row.of("id3", 111L),
        TestHelpers.Row.of("id2", 222L),
        TestHelpers.Row.of("id1", 111L));
  }

  @Test
  public void testTimestamp() {
    Schema schemaWithZone = singleSortFildSchema(Types.TimestampType.withZone());
    Schema schemaWithoutZone = singleSortFildSchema(Types.TimestampType.withoutZone());

    long lessMicro =
        TimeUnit.SECONDS.toMicros(
            LocalDateTime.of(2022, 1, 10, 0, 0, 0).toEpochSecond(ZoneOffset.UTC));
    long greaterMicro =
        TimeUnit.SECONDS.toMicros(
            LocalDateTime.of(2022, 1, 10, 1, 0, 0).toEpochSecond(ZoneOffset.UTC));
    long lessCopyMicro =
        TimeUnit.SECONDS.toMicros(
            LocalDateTime.of(2022, 1, 10, 0, 0, 0).toEpochSecond(ZoneOffset.UTC));

    TestHelpers.Row less = TestHelpers.Row.of("id3", lessMicro);
    TestHelpers.Row greater = TestHelpers.Row.of("id2", greaterMicro);
    TestHelpers.Row lessCopy = TestHelpers.Row.of("id1", lessCopyMicro);
    testIdentitySortField(schemaWithZone, less, greater, lessCopy);
    testIdentitySortField(schemaWithoutZone, less, greater, lessCopy);
  }

  @Test
  public void testTimestampTransform() {
    Schema schema = singleSortFildSchema(Types.TimestampType.withZone());
    Transform<Long, Integer> bucket = Transforms.day();
    SerializableFunction<Long, Integer> transform = bucket.bind(Types.TimestampType.withZone());

    long lessMicro =
        TimeUnit.SECONDS.toMicros(
            LocalDateTime.of(2022, 1, 10, 0, 0, 0).toEpochSecond(ZoneOffset.UTC));
    long greaterMicro =
        TimeUnit.SECONDS.toMicros(
            LocalDateTime.of(2022, 1, 11, 0, 0, 0).toEpochSecond(ZoneOffset.UTC));
    // intentionally changed hour value from 0 to 1. days should equal
    long lessCopyMicro =
        TimeUnit.SECONDS.toMicros(
            LocalDateTime.of(2022, 1, 10, 1, 0, 0).toEpochSecond(ZoneOffset.UTC));

    assertThat(transform.apply(lessMicro)).isLessThan(transform.apply(greaterMicro));
    assertThat(transform.apply(lessMicro)).isEqualTo(transform.apply(lessCopyMicro));

    TestHelpers.Row less = TestHelpers.Row.of("id3", lessMicro);
    TestHelpers.Row greater = TestHelpers.Row.of("id2", greaterMicro);
    TestHelpers.Row lessCopy = TestHelpers.Row.of("id1", lessCopyMicro);

    SortOrder sortOrder =
        SortOrder.builderFor(schema)
            .sortBy(Expressions.day("field"), SortDirection.ASC, NullOrder.NULLS_FIRST)
            .build();
    assertComparesCorrectly(
        schema, sortOrder, less, greater, lessCopy, SINGLE_SORT_FILED_NULL_VALUE);
  }

  @Test
  public void testString() {
    testIdentitySortField(
        singleSortFildSchema(Types.StringType.get()),
        TestHelpers.Row.of("id3", "aaa"),
        TestHelpers.Row.of("id2", "bbb"),
        TestHelpers.Row.of("id1", "aaa"));
  }

  @Test
  public void testStringBucket() {
    Schema schema = singleSortFildSchema(Types.StringType.get());
    Transform<String, Integer> bucket = Transforms.bucket(4);
    SerializableFunction<String, Integer> transform = bucket.bind(Types.StringType.get());

    assertThat(transform.apply("bbb")).isLessThan(transform.apply("aaa"));
    assertThat(transform.apply("bbb")).isEqualTo(transform.apply("cca"));

    TestHelpers.Row less = TestHelpers.Row.of("id3", "bbb");
    TestHelpers.Row greater = TestHelpers.Row.of("id2", "aaa");
    // bucket("cca", 4) equals to bucket("bbb", 4)
    TestHelpers.Row lessCopy = TestHelpers.Row.of("id1", "cca");

    SortOrder sortOrder =
        SortOrder.builderFor(schema)
            .sortBy(Expressions.bucket("field", 4), SortDirection.ASC, NullOrder.NULLS_FIRST)
            .build();
    assertComparesCorrectly(
        schema, sortOrder, less, greater, lessCopy, SINGLE_SORT_FILED_NULL_VALUE);
  }

  @Test
  public void testUuid() {
    Schema schema = singleSortFildSchema(Types.UUIDType.get());
    TestHelpers.Row less =
        TestHelpers.Row.of("id3", UUID.fromString("81873e7d-1374-4493-8e1d-9095eff7046c"));
    TestHelpers.Row greater =
        TestHelpers.Row.of("id2", UUID.fromString("fd02441d-1423-4a3f-8785-c7dd5647e26b"));
    TestHelpers.Row lessCopy =
        TestHelpers.Row.of("id1", UUID.fromString("81873e7d-1374-4493-8e1d-9095eff7046c"));
    testIdentitySortField(schema, less, greater, lessCopy);
  }

  @Test
  public void testUUIDBucket() {
    Schema schema = singleSortFildSchema(Types.UUIDType.get());
    Transform<UUID, Integer> bucket = Transforms.bucket(4);
    SerializableFunction<UUID, Integer> transform = bucket.bind(Types.UUIDType.get());

    assertThat(transform.apply(UUID.fromString("fd02441d-1423-4a3f-8785-c7dd5647e26b")))
        .isLessThan(transform.apply(UUID.fromString("86873e7d-1374-4493-8e1d-9095eff7046c")));
    assertThat(transform.apply(UUID.fromString("fd02441d-1423-4a3f-8785-c7dd5647e26b")))
        .isEqualTo(transform.apply(UUID.fromString("81873e7d-1374-4493-8e1d-9095eff7046c")));

    TestHelpers.Row less =
        TestHelpers.Row.of("id3", UUID.fromString("fd02441d-1423-4a3f-8785-c7dd5647e26b"));
    TestHelpers.Row greater =
        TestHelpers.Row.of("id2", UUID.fromString("86873e7d-1374-4493-8e1d-9095eff7046c"));
    // bucket value equals to the less object
    TestHelpers.Row lessCopy =
        TestHelpers.Row.of("id1", UUID.fromString("81873e7d-1374-4493-8e1d-9095eff7046c"));

    SortOrder sortOrder =
        SortOrder.builderFor(schema)
            .sortBy(Expressions.bucket("field", 4), SortDirection.ASC, NullOrder.NULLS_FIRST)
            .build();
    assertComparesCorrectly(
        schema, sortOrder, less, greater, lessCopy, SINGLE_SORT_FILED_NULL_VALUE);
  }

  @Test
  public void testFixed() {
    testIdentitySortField(
        singleSortFildSchema(Types.FixedType.ofLength(3)),
        TestHelpers.Row.of("id3", ByteBuffer.wrap(new byte[] {1, 2, 3})),
        TestHelpers.Row.of("id2", ByteBuffer.wrap(new byte[] {3, 2, 1})),
        TestHelpers.Row.of("id1", ByteBuffer.wrap(new byte[] {1, 2, 3})));
  }

  @Test
  public void testBinary() {
    testIdentitySortField(
        singleSortFildSchema(Types.BinaryType.get()),
        TestHelpers.Row.of("id3", ByteBuffer.wrap(new byte[] {1, 1})),
        TestHelpers.Row.of("id2", ByteBuffer.wrap(new byte[] {1, 1, 1})),
        TestHelpers.Row.of("id1", ByteBuffer.wrap(new byte[] {1, 1})));
  }

  @Test
  public void testBinaryTruncate() {
    Schema schema = singleSortFildSchema(Types.BinaryType.get());
    Transform<ByteBuffer, ByteBuffer> truncate = Transforms.truncate(2);
    SerializableFunction<ByteBuffer, ByteBuffer> transform = truncate.bind(Types.BinaryType.get());

    assertThat(transform.apply(ByteBuffer.wrap(new byte[] {1, 2, 3})))
        .isLessThan(transform.apply(ByteBuffer.wrap(new byte[] {1, 3, 1})));
    assertThat(transform.apply(ByteBuffer.wrap(new byte[] {1, 2, 3})))
        .isEqualTo(transform.apply(ByteBuffer.wrap(new byte[] {1, 2, 5, 6})));

    TestHelpers.Row less = TestHelpers.Row.of("id3", ByteBuffer.wrap(new byte[] {1, 2, 3}));
    TestHelpers.Row greater = TestHelpers.Row.of("id2", ByteBuffer.wrap(new byte[] {1, 3, 1}));
    // bucket value equals to the less object
    TestHelpers.Row lessCopy = TestHelpers.Row.of("id1", ByteBuffer.wrap(new byte[] {1, 2, 5, 6}));

    SortOrder sortOrder =
        SortOrder.builderFor(schema)
            .sortBy(Expressions.truncate("field", 2), SortDirection.ASC, NullOrder.NULLS_FIRST)
            .build();
    assertComparesCorrectly(
        schema, sortOrder, less, greater, lessCopy, SINGLE_SORT_FILED_NULL_VALUE);
  }

  @Test
  public void testDecimal() {
    testIdentitySortField(
        singleSortFildSchema(Types.DecimalType.of(9, 5)),
        TestHelpers.Row.of("id3", BigDecimal.valueOf(0.1)),
        TestHelpers.Row.of("id2", BigDecimal.valueOf(0.2)),
        TestHelpers.Row.of("id1", BigDecimal.valueOf(0.1)));
  }

  @Test
  public void testStruct() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.StringType.get()),
            Types.NestedField.optional(
                2,
                "location",
                Types.StructType.of(
                    Types.NestedField.required(11, "lat", Types.FloatType.get()),
                    Types.NestedField.required(12, "long", Types.FloatType.get()))));

    SortOrder latOnlyAsc = SortOrder.builderFor(schema).asc("location.lat").build();
    TestHelpers.Row lessLat = TestHelpers.Row.of("id4", TestHelpers.Row.of(1.0f, 1.0f));
    TestHelpers.Row greaterLat = TestHelpers.Row.of("id3", TestHelpers.Row.of(2.0f, 1.0f));
    TestHelpers.Row lessLatCopy = TestHelpers.Row.of("id2", TestHelpers.Row.of(1.0f, 1.0f));
    TestHelpers.Row nullLat = TestHelpers.Row.of("id1", TestHelpers.Row.of(null, 1.0f));
    assertComparesCorrectly(schema, latOnlyAsc, lessLat, greaterLat, lessLatCopy, nullLat);

    SortOrder longOnlyDesc =
        SortOrder.builderFor(schema).desc("location.long", NullOrder.NULLS_LAST).build();
    TestHelpers.Row lessLong = TestHelpers.Row.of("id4", TestHelpers.Row.of(1.0f, 1.0f));
    TestHelpers.Row greaterLong = TestHelpers.Row.of("id3", TestHelpers.Row.of(1.0f, 2.0f));
    TestHelpers.Row lessLongCopy = TestHelpers.Row.of("id2", TestHelpers.Row.of(1.0f, 1.0f));
    TestHelpers.Row nullLong = TestHelpers.Row.of("id1", TestHelpers.Row.of(1.0f, null));
    assertComparesCorrectly(schema, longOnlyDesc, lessLong, greaterLong, lessLongCopy, nullLong);

    SortOrder twoFieldsSortOrder =
        SortOrder.builderFor(schema).asc("location.lat").asc("location.long").build();
    TestHelpers.Row lessTwoSortFields = TestHelpers.Row.of("id4", TestHelpers.Row.of(1.0f, 1.0f));
    TestHelpers.Row greaterLatTwoSortFields =
        TestHelpers.Row.of("id3", TestHelpers.Row.of(2.0f, 1.0f));
    TestHelpers.Row greaterLongTwoSortFields =
        TestHelpers.Row.of("id3", TestHelpers.Row.of(1.0f, 2.0f));
    TestHelpers.Row greaterBothTwoSortFields =
        TestHelpers.Row.of("id3", TestHelpers.Row.of(2.0f, 2.0f));
    TestHelpers.Row lessTwoSortFieldsCopy =
        TestHelpers.Row.of("id2", TestHelpers.Row.of(1.0f, 1.0f));
    TestHelpers.Row nullLatTwoSortFields =
        TestHelpers.Row.of("id1", TestHelpers.Row.of(null, 1.0f));
    TestHelpers.Row nullLongTwoSortFields =
        TestHelpers.Row.of("id1", TestHelpers.Row.of(1.0f, null));
    TestHelpers.Row nullBothTowSortFields =
        TestHelpers.Row.of("id1", TestHelpers.Row.of(null, null));
    assertComparesCorrectly(
        schema,
        twoFieldsSortOrder,
        lessTwoSortFields,
        greaterLatTwoSortFields,
        lessTwoSortFieldsCopy,
        nullLatTwoSortFields);
    assertComparesCorrectly(
        schema,
        twoFieldsSortOrder,
        lessTwoSortFields,
        greaterLongTwoSortFields,
        lessTwoSortFieldsCopy,
        nullLongTwoSortFields);
    assertComparesCorrectly(
        schema,
        twoFieldsSortOrder,
        lessTwoSortFields,
        greaterBothTwoSortFields,
        lessTwoSortFieldsCopy,
        nullBothTowSortFields);
  }

  @Test
  public void testStructTransform() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.StringType.get()),
            Types.NestedField.optional(
                2,
                "struct",
                Types.StructType.of(
                    Types.NestedField.required(11, "left", Types.BinaryType.get()),
                    Types.NestedField.required(12, "right", Types.BinaryType.get()))));

    Transform<ByteBuffer, ByteBuffer> bucket = Transforms.truncate(2);
    SerializableFunction<ByteBuffer, ByteBuffer> transform = bucket.bind(Types.BinaryType.get());

    assertThat(transform.apply(ByteBuffer.wrap(new byte[] {2, 3, 4})))
        .isLessThan(transform.apply(ByteBuffer.wrap(new byte[] {9, 3, 4})));
    assertThat(transform.apply(ByteBuffer.wrap(new byte[] {2, 3, 4})))
        .isEqualTo(transform.apply(ByteBuffer.wrap(new byte[] {2, 3, 9})));

    TestHelpers.Row less =
        TestHelpers.Row.of(
            "id4",
            TestHelpers.Row.of(
                ByteBuffer.wrap(new byte[] {1}), ByteBuffer.wrap(new byte[] {2, 3, 4})));
    TestHelpers.Row greater =
        TestHelpers.Row.of(
            "id3",
            TestHelpers.Row.of(
                ByteBuffer.wrap(new byte[] {1}), ByteBuffer.wrap(new byte[] {9, 3, 4})));
    TestHelpers.Row lessCopy =
        TestHelpers.Row.of(
            "id2",
            TestHelpers.Row.of(
                ByteBuffer.wrap(new byte[] {1}), ByteBuffer.wrap(new byte[] {2, 3, 9})));
    TestHelpers.Row nullRight =
        TestHelpers.Row.of("id1", TestHelpers.Row.of(ByteBuffer.wrap(new byte[] {1}), null));

    SortOrder sortOrder =
        SortOrder.builderFor(schema)
            .sortBy(
                Expressions.truncate("struct.left", 2), SortDirection.ASC, NullOrder.NULLS_FIRST)
            .sortBy(
                Expressions.truncate("struct.right", 2), SortDirection.ASC, NullOrder.NULLS_FIRST)
            .build();
    assertComparesCorrectly(schema, sortOrder, less, greater, lessCopy, nullRight);
  }

  @Test
  public void testNestedStruct() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.StringType.get()),
            Types.NestedField.optional(
                2,
                "user",
                Types.StructType.of(
                    Types.NestedField.required(11, "name", Types.StringType.get()),
                    Types.NestedField.optional(
                        12,
                        "location",
                        Types.StructType.of(
                            Types.NestedField.required(101, "lat", Types.FloatType.get()),
                            Types.NestedField.required(102, "long", Types.FloatType.get()))))));

    SortOrder build =
        SortOrder.builderFor(schema).asc("user.location.lat").asc("user.location.long").build();
    TestHelpers.Row less =
        TestHelpers.Row.of("id4", TestHelpers.Row.of("user4", TestHelpers.Row.of(1.0f, 1.0f)));
    TestHelpers.Row greaterLat =
        TestHelpers.Row.of("id3", TestHelpers.Row.of("user3", TestHelpers.Row.of(2.0f, 1.0f)));
    TestHelpers.Row greaterLong =
        TestHelpers.Row.of("id3", TestHelpers.Row.of("user3", TestHelpers.Row.of(1.0f, 2.0f)));
    TestHelpers.Row greaterBoth =
        TestHelpers.Row.of("id3", TestHelpers.Row.of("user3", TestHelpers.Row.of(2.0f, 2.0f)));
    TestHelpers.Row lessCopy =
        TestHelpers.Row.of("id2", TestHelpers.Row.of("user2", TestHelpers.Row.of(1.0f, 1.0f)));
    TestHelpers.Row nullLat =
        TestHelpers.Row.of("id1", TestHelpers.Row.of("user1", TestHelpers.Row.of(null, 1.0f)));
    TestHelpers.Row nullLong =
        TestHelpers.Row.of("id1", TestHelpers.Row.of("user1", TestHelpers.Row.of(1.0f, null)));
    TestHelpers.Row nullBoth =
        TestHelpers.Row.of("id1", TestHelpers.Row.of("user1", TestHelpers.Row.of(null, null)));
    assertComparesCorrectly(schema, build, less, greaterLat, lessCopy, nullLat);
    assertComparesCorrectly(schema, build, less, greaterLong, lessCopy, nullLong);
    assertComparesCorrectly(schema, build, less, greaterBoth, lessCopy, nullBoth);
  }
}
