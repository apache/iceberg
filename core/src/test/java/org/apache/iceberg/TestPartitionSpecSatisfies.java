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

import static org.apache.iceberg.expressions.Expressions.bucket;
import static org.apache.iceberg.expressions.Expressions.day;
import static org.apache.iceberg.expressions.Expressions.hour;
import static org.apache.iceberg.expressions.Expressions.month;
import static org.apache.iceberg.expressions.Expressions.truncate;
import static org.apache.iceberg.expressions.Expressions.year;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestPartitionSpecSatisfies extends TestBase {
  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.required(2, "ts", Types.TimestampType.withZone()),
          Types.NestedField.required(3, "category", Types.StringType.get()),
          Types.NestedField.optional(4, "data", Types.StringType.get()));

  private static final PartitionSpec UNPARTITIONED = PartitionSpec.builderFor(SCHEMA).build();
  private static final PartitionSpec PARTITIONED =
      PartitionSpec.builderFor(SCHEMA)
          .identity("category")
          .day("ts")
          .bucket("id", 16, "shard")
          .build();

  @TestTemplate
  public void testUnpartitioned() {
    assertThat(UNPARTITIONED.satisfies(UNPARTITIONED)).isTrue();
  }

  @TestTemplate
  public void testPartitionedSatisfiesUnpartitioned() {
    assertThat(PARTITIONED.satisfies(UNPARTITIONED)).isTrue();
  }

  @TestTemplate
  void testUnpartitionedDoesNotSatisfyPartitioned() {
    assertThat(UNPARTITIONED.satisfies(PARTITIONED)).isFalse();
  }

  @TestTemplate
  void testSameSpecSatisfiesItself() {
    assertThat(PARTITIONED.satisfies(PARTITIONED)).isTrue();
  }

  @TestTemplate
  void testDifferentSourceColumnDoesNotSatisfy() {
    PartitionSpec byCategory =
        new BaseUpdatePartitionSpec(formatVersion, UNPARTITIONED)
            .addField(org.apache.iceberg.expressions.Expressions.ref("category"))
            .apply();
    PartitionSpec byDay =
        new BaseUpdatePartitionSpec(formatVersion, UNPARTITIONED).addField(day("ts")).apply();
    assertThat(byDay.satisfies(byCategory)).isFalse();
    assertThat(byCategory.satisfies(byDay)).isFalse();
  }

  @TestTemplate
  void testHourSatisfiesMonth() {
    PartitionSpec byMonth =
        new BaseUpdatePartitionSpec(formatVersion, UNPARTITIONED).addField(month("ts")).apply();
    PartitionSpec byHour =
        new BaseUpdatePartitionSpec(formatVersion, UNPARTITIONED).addField(hour("ts")).apply();
    assertThat(byHour.satisfies(byMonth)).isTrue();
  }

  @TestTemplate
  void testHourSatisfiedDayAndMonth() {
    PartitionSpec byMonth =
        new BaseUpdatePartitionSpec(formatVersion, UNPARTITIONED).addField(month("ts")).apply();
    PartitionSpec byMonthAndDay =
        new BaseUpdatePartitionSpec(formatVersion, byMonth).addField(day("ts")).apply();
    PartitionSpec byHour =
        new BaseUpdatePartitionSpec(formatVersion, UNPARTITIONED).addField(hour("ts")).apply();
    assertThat(byHour.satisfies(byMonthAndDay)).isTrue();
  }

  @TestTemplate
  void testYearDoesNotSatisfyMonth() {
    PartitionSpec byMonth =
        new BaseUpdatePartitionSpec(formatVersion, UNPARTITIONED).addField(month("ts")).apply();
    PartitionSpec byYear =
        new BaseUpdatePartitionSpec(formatVersion, UNPARTITIONED).addField(year("ts")).apply();
    assertThat(byYear.satisfies(byMonth)).isFalse();
  }

  @TestTemplate
  void testHourSatisfyYear() {
    PartitionSpec byYear =
        new BaseUpdatePartitionSpec(formatVersion, UNPARTITIONED).addField(year("ts")).apply();
    PartitionSpec byHour =
        new BaseUpdatePartitionSpec(formatVersion, UNPARTITIONED).addField(hour("ts")).apply();
    // hour is finer than year, so hour DOES satisfy year
    assertThat(byHour.satisfies(byYear)).isTrue();
  }

  @TestTemplate
  void testMultiFieldBothTransformsSatisfy() {
    PartitionSpec byCategoryAndMonth =
        new BaseUpdatePartitionSpec(formatVersion, UNPARTITIONED).addField(month("ts")).apply();
    byCategoryAndMonth =
        new BaseUpdatePartitionSpec(formatVersion, byCategoryAndMonth)
            .addField("category_field", org.apache.iceberg.expressions.Expressions.ref("category"))
            .apply();

    PartitionSpec byCategoryAndDay =
        new BaseUpdatePartitionSpec(formatVersion, UNPARTITIONED).addField(day("ts")).apply();
    byCategoryAndDay =
        new BaseUpdatePartitionSpec(formatVersion, byCategoryAndDay)
            .addField("category_field", org.apache.iceberg.expressions.Expressions.ref("category"))
            .apply();

    // day is finer than month, and identity satisfies identity, so this should satisfy
    assertThat(byCategoryAndDay.satisfies(byCategoryAndMonth)).isTrue();
  }

  @TestTemplate
  void testMultiFieldOneTransformDoesNotSatisfy() {
    PartitionSpec byCategoryAndDay =
        new BaseUpdatePartitionSpec(formatVersion, UNPARTITIONED).addField(day("ts")).apply();
    byCategoryAndDay =
        new BaseUpdatePartitionSpec(formatVersion, byCategoryAndDay)
            .addField("category_field", org.apache.iceberg.expressions.Expressions.ref("category"))
            .apply();

    PartitionSpec byCategoryAndHour =
        new BaseUpdatePartitionSpec(formatVersion, UNPARTITIONED).addField(hour("ts")).apply();
    byCategoryAndHour =
        new BaseUpdatePartitionSpec(formatVersion, byCategoryAndHour)
            .addField("category_field", org.apache.iceberg.expressions.Expressions.ref("category"))
            .apply();

    // day is coarser than hour, so day does NOT satisfy hour
    assertThat(byCategoryAndDay.satisfies(byCategoryAndHour)).isFalse();
  }

  @TestTemplate
  void testTruncateWiderSatisfiesNarrower() {
    PartitionSpec byTruncate5 =
        new BaseUpdatePartitionSpec(formatVersion, UNPARTITIONED)
            .addField(truncate("data", 5))
            .apply();
    PartitionSpec byTruncate10 =
        new BaseUpdatePartitionSpec(formatVersion, UNPARTITIONED)
            .addField(truncate("data", 10))
            .apply();
    // truncate(10) preserves more information than truncate(5), so truncate(10) satisfies
    // truncate(5)
    assertThat(byTruncate10.satisfies(byTruncate5)).isTrue();
  }

  @TestTemplate
  void testTruncateNarrowerDoesNotSatisfyWider() {
    PartitionSpec byTruncate5 =
        new BaseUpdatePartitionSpec(formatVersion, UNPARTITIONED)
            .addField(truncate("data", 5))
            .apply();
    PartitionSpec byTruncate10 =
        new BaseUpdatePartitionSpec(formatVersion, UNPARTITIONED)
            .addField(truncate("data", 10))
            .apply();
    assertThat(byTruncate5.satisfies(byTruncate10)).isFalse();
  }

  @TestTemplate
  void testBucketDifferentWidthDoesNotSatisfy() {
    PartitionSpec byBucket16 =
        new BaseUpdatePartitionSpec(formatVersion, UNPARTITIONED)
            .addField(bucket("id", 16))
            .apply();
    PartitionSpec byBucket32 =
        new BaseUpdatePartitionSpec(formatVersion, UNPARTITIONED)
            .addField(bucket("id", 32))
            .apply();
    // different bucket widths produce different partitions — neither satisfies the other
    assertThat(byBucket32.satisfies(byBucket16)).isFalse();
    assertThat(byBucket16.satisfies(byBucket32)).isFalse();
  }

  @TestTemplate
  void testBucketSameWidthSatisfies() {
    PartitionSpec byBucket16a =
        new BaseUpdatePartitionSpec(formatVersion, UNPARTITIONED)
            .addField(bucket("id", 16))
            .apply();
    PartitionSpec byBucket16b =
        new BaseUpdatePartitionSpec(formatVersion, UNPARTITIONED)
            .addField(bucket("id", 16))
            .apply();
    assertThat(byBucket16a.satisfies(byBucket16b)).isTrue();
  }

  @TestTemplate
  void testIdentityOnTimestampSatisfiesDay() {
    PartitionSpec byDay =
        new BaseUpdatePartitionSpec(formatVersion, UNPARTITIONED).addField(day("ts")).apply();
    PartitionSpec byIdentityTs =
        new BaseUpdatePartitionSpec(formatVersion, UNPARTITIONED).addField("ts").apply();
    assertThat(byIdentityTs.satisfies(byDay)).isTrue();
  }

  @TestTemplate
  void testDayDoesNotSatisfyIdentityOnTimestamp() {
    PartitionSpec byIdentityTs =
        new BaseUpdatePartitionSpec(formatVersion, UNPARTITIONED).addField("ts").apply();
    PartitionSpec byDay =
        new BaseUpdatePartitionSpec(formatVersion, UNPARTITIONED).addField(day("ts")).apply();
    assertThat(byDay.satisfies(byIdentityTs)).isFalse();
  }

  // month is coarser than day — month does NOT satisfy day
  @TestTemplate
  void testMonthDoesNotSatisfyDay() {
    PartitionSpec byDay =
        new BaseUpdatePartitionSpec(formatVersion, UNPARTITIONED).addField(day("ts")).apply();
    PartitionSpec byMonth =
        new BaseUpdatePartitionSpec(formatVersion, UNPARTITIONED).addField(month("ts")).apply();
    assertThat(byMonth.satisfies(byDay)).isFalse();
  }

  // a spec with extra fields still satisfies a spec with a strict subset of those fields
  @TestTemplate
  void testSpecWithExtraFieldSatisfiesSubset() {
    PartitionSpec byDay =
        new BaseUpdatePartitionSpec(formatVersion, UNPARTITIONED).addField(day("ts")).apply();
    PartitionSpec byCategoryAndDayAndBucket =
        new BaseUpdatePartitionSpec(formatVersion, byDay).addField("category").apply();
    byCategoryAndDayAndBucket =
        new BaseUpdatePartitionSpec(formatVersion, byCategoryAndDayAndBucket)
            .addField(bucket("id", 16))
            .apply();
    assertThat(byCategoryAndDayAndBucket.satisfies(byDay)).isTrue();
  }

  // spec with fewer fields does NOT satisfy a spec that requires more
  @TestTemplate
  void testSpecWithFewerFieldsDoesNotSatisfyMore() {
    PartitionSpec byCategoryAndDay =
        new BaseUpdatePartitionSpec(formatVersion, UNPARTITIONED).addField("category").apply();
    byCategoryAndDay =
        new BaseUpdatePartitionSpec(formatVersion, byCategoryAndDay).addField(day("ts")).apply();
    PartitionSpec byDayOnly =
        new BaseUpdatePartitionSpec(formatVersion, UNPARTITIONED).addField(day("ts")).apply();
    assertThat(byDayOnly.satisfies(byCategoryAndDay)).isFalse();
  }

  // truncate on a different column does NOT satisfy truncate on another column
  @TestTemplate
  void testTruncateDifferentColumnDoesNotSatisfy() {
    PartitionSpec byTruncateData =
        new BaseUpdatePartitionSpec(formatVersion, UNPARTITIONED)
            .addField(truncate("data", 5))
            .apply();
    PartitionSpec byTruncateCategory =
        new BaseUpdatePartitionSpec(formatVersion, UNPARTITIONED)
            .addField(truncate("category", 5))
            .apply();
    assertThat(byTruncateData.satisfies(byTruncateCategory)).isFalse();
    assertThat(byTruncateCategory.satisfies(byTruncateData)).isFalse();
  }

  @TestTemplate
  public void testAddOnePartition() {
    PartitionSpec byDay =
        new BaseUpdatePartitionSpec(formatVersion, UNPARTITIONED).addField(day("ts")).apply();
    assertThat(byDay.satisfies(UNPARTITIONED)).isTrue();
    PartitionSpec byBucket =
        new BaseUpdatePartitionSpec(formatVersion, UNPARTITIONED)
            .addField(bucket("id", 16))
            .apply();
    assertThat(byBucket.satisfies(UNPARTITIONED)).isTrue();
  }

  @TestTemplate
  public void testEvolveDayOnMonth() {
    PartitionSpec byMonth =
        new BaseUpdatePartitionSpec(formatVersion, UNPARTITIONED).addField(month("ts")).apply();
    PartitionSpec byDay =
        new BaseUpdatePartitionSpec(formatVersion, byMonth).addField(day("ts")).apply();
    assertThat(byDay.satisfies(byMonth)).isTrue();
  }

  @TestTemplate
  public void testEvolveHourOnDay() {
    PartitionSpec byDay =
        new BaseUpdatePartitionSpec(formatVersion, UNPARTITIONED).addField(day("ts")).apply();
    PartitionSpec byHour =
        new BaseUpdatePartitionSpec(formatVersion, byDay).addField(hour("ts")).apply();
    assertThat(byHour.satisfies(byDay)).isTrue();
  }

  @TestTemplate
  public void testDataBucketOnDay() {
    PartitionSpec byDay =
        new BaseUpdatePartitionSpec(formatVersion, UNPARTITIONED).addField(day("ts")).apply();
    PartitionSpec byDayByDataBucket =
        new BaseUpdatePartitionSpec(formatVersion, byDay).addField(bucket("data", 16)).apply();
    assertThat(byDayByDataBucket.satisfies(byDay)).isTrue();
  }

  @TestTemplate
  public void testDayEvolveOnMonth() {
    PartitionSpec byDay =
        new BaseUpdatePartitionSpec(formatVersion, UNPARTITIONED).addField(day("ts")).apply();
    PartitionSpec byMonth =
        new BaseUpdatePartitionSpec(formatVersion, byDay).addField(month("ts")).apply();
    assertThat(byMonth.satisfies(byDay)).isTrue();
  }

  @TestTemplate
  public void testDayHourCombination() {
    PartitionSpec byDay =
        new BaseUpdatePartitionSpec(formatVersion, UNPARTITIONED).addField(day("ts")).apply();
    PartitionSpec byHour =
        new BaseUpdatePartitionSpec(formatVersion, UNPARTITIONED).addField(hour("ts")).apply();
    assertThat(byHour.satisfies(byDay)).isTrue();
    assertThat(byDay.satisfies(byHour)).isFalse();
  }

  @TestTemplate
  public void testEvolveHourOnDayAndMonth() {
    PartitionSpec byMonth =
        new BaseUpdatePartitionSpec(formatVersion, UNPARTITIONED).addField(month("ts")).apply();
    PartitionSpec byDay =
        new BaseUpdatePartitionSpec(formatVersion, byMonth).addField(day("ts")).apply();
    PartitionSpec byHour =
        new BaseUpdatePartitionSpec(formatVersion, byDay).addField(hour("ts")).apply();
    assertThat(byHour.satisfies(byDay)).isTrue();
    assertThat(byHour.satisfies(byMonth)).isTrue();
  }

  @TestTemplate
  public void testRemoveField() {
    PartitionSpec byCategoryAndDay =
        new BaseUpdatePartitionSpec(formatVersion, UNPARTITIONED)
            .addField("category_field", org.apache.iceberg.expressions.Expressions.ref("category"))
            .addField(day("ts"))
            .apply();
    PartitionSpec byCategoryOnly =
        new BaseUpdatePartitionSpec(formatVersion, byCategoryAndDay).removeField(day("ts")).apply();
    assertThat(byCategoryOnly.satisfies(byCategoryAndDay)).isFalse();
    PartitionSpec byDayOnly =
        new BaseUpdatePartitionSpec(formatVersion, byCategoryAndDay)
            .removeField("category_field")
            .apply();
    assertThat(byDayOnly.satisfies(byCategoryAndDay)).isFalse();
  }

  @TestTemplate
  public void testRemoveFieldButKeepSameLength() {
    PartitionSpec byCategoryAndDay =
        new BaseUpdatePartitionSpec(formatVersion, UNPARTITIONED)
            .addField("category_field", org.apache.iceberg.expressions.Expressions.ref("category"))
            .addField(day("ts"))
            .apply();
    PartitionSpec byCategoryAndDayOnly =
        new BaseUpdatePartitionSpec(formatVersion, byCategoryAndDay)
            .removeField(day("ts"))
            .addField(month("ts"))
            .apply();
    assertThat(byCategoryAndDayOnly.satisfies(byCategoryAndDay)).isFalse();

    PartitionSpec byDayAndMonthOnly =
        new BaseUpdatePartitionSpec(formatVersion, byCategoryAndDay)
            .removeField("category_field")
            .addField(month("ts"))
            .apply();
    assertThat(byDayAndMonthOnly.satisfies(byCategoryAndDay)).isFalse();
  }

  @TestTemplate
  public void testVoidTransformField() {
    PartitionSpec initial =
        new BaseUpdatePartitionSpec(formatVersion, UNPARTITIONED)
            .addField(day("ts"))
            .addField("category_field", org.apache.iceberg.expressions.Expressions.ref("category"))
            .apply();

    PartitionSpec voidSpec =
        new BaseUpdatePartitionSpec(formatVersion, initial)
            .removeField(day("ts"))
            .removeField("category_field")
            .apply();
    assertThat(voidSpec.satisfies(initial)).isFalse();

    PartitionSpec byDayOnVoid =
        new BaseUpdatePartitionSpec(formatVersion, voidSpec).addField(day("ts")).apply();
    assertThat(byDayOnVoid.satisfies(voidSpec)).isTrue();
  }
}
