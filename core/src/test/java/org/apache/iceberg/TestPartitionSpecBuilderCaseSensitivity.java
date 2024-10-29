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

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestPartitionSpecBuilderCaseSensitivity {

  private static final int V2_FORMAT_VERSION = 2;
  private static final Schema SCHEMA_WITHOUT_NAME_CONFLICTS =
      new Schema(
          required(1, "id", Types.IntegerType.get()),
          required(2, "data", Types.StringType.get()),
          required(3, "category", Types.StringType.get()),
          required(4, "order_date", Types.DateType.get()),
          required(5, "order_time", Types.TimestampType.withoutZone()),
          required(6, "ship_date", Types.DateType.get()),
          required(7, "ship_time", Types.TimestampType.withoutZone()));

  private static final Schema SCHEMA_WITH_NAME_CONFLICTS =
      new Schema(
          required(1, "id", Types.IntegerType.get()),
          required(2, "data", Types.StringType.get()),
          required(3, "DATA", Types.StringType.get()),
          required(4, "order_date", Types.DateType.get()),
          required(5, "ORDER_DATE", Types.DateType.get()),
          required(6, "order_time", Types.TimestampType.withoutZone()),
          required(7, "ORDER_TIME", Types.TimestampType.withoutZone()));

  @TempDir private Path temp;
  private File tableDir = null;

  @BeforeEach
  public void setupTableDir() throws IOException {
    this.tableDir = Files.createTempDirectory(temp, "junit").toFile();
  }

  @AfterEach
  public void cleanupTables() {
    TestTables.clearTables();
  }

  @Test
  public void testPartitionTypeWithColumnNamesThatDifferOnlyInLetterCase() {
    Schema schema =
        new Schema(
            required(1, "id", Types.IntegerType.get()),
            required(2, "data", Types.StringType.get()),
            required(3, "DATA", Types.StringType.get()),
            required(4, "order_date", Types.DateType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("data").identity("DATA").build();

    StructType expectedType =
        StructType.of(
            NestedField.optional(1000, "data", Types.StringType.get()),
            NestedField.optional(1001, "DATA", Types.StringType.get()));
    StructType actualType = spec.partitionType();
    assertThat(actualType).isEqualTo(expectedType);
  }

  @Test
  public void testPartitionTypeWithIdentityTargetName() {
    PartitionSpec spec =
        PartitionSpec.builderFor(SCHEMA_WITHOUT_NAME_CONFLICTS)
            .identity("data", "partition1")
            .build();

    StructType expectedType =
        StructType.of(NestedField.optional(1000, "partition1", Types.StringType.get()));
    StructType actualType = spec.partitionType();
    assertThat(actualType).isEqualTo(expectedType);
  }

  @Test
  public void testBucketSourceNameAllowsExactDuplicateWhenCaseSensitive() {
    PartitionSpec spec =
        PartitionSpec.builderFor(SCHEMA_WITH_NAME_CONFLICTS)
            .bucket("data", 10, "partition1")
            .bucket("data", 10, "PARTITION1")
            .build();

    StructType expectedType =
        StructType.of(
            NestedField.optional(1000, "partition1", Types.IntegerType.get()),
            NestedField.optional(1001, "PARTITION1", Types.IntegerType.get()));
    StructType actualType = spec.partitionType();
    assertThat(actualType).isEqualTo(expectedType);
  }

  @Test
  public void testBucketTargetNameDefaultValue() {
    PartitionSpec spec =
        PartitionSpec.builderFor(SCHEMA_WITH_NAME_CONFLICTS).bucket("data", 10).build();

    StructType expectedType =
        StructType.of(NestedField.optional(1000, "data_bucket", Types.IntegerType.get()));
    StructType actualType = spec.partitionType();
    assertThat(actualType).isEqualTo(expectedType);
  }

  @Test
  public void testBucketTargetNameDefaultValueCaseInsensitive() {
    PartitionSpec spec =
        PartitionSpec.builderFor(SCHEMA_WITHOUT_NAME_CONFLICTS)
            .caseSensitive(false)
            .bucket("DATA", 10)
            .build();

    StructType expectedType =
        StructType.of(NestedField.optional(1000, "data_bucket", Types.IntegerType.get()));
    StructType actualType = spec.partitionType();
    assertThat(actualType).isEqualTo(expectedType);
  }

  @Test
  public void testBucketSourceNameAllowsInexactDuplicateWhenCaseInsensitive() {
    PartitionSpec spec =
        PartitionSpec.builderFor(SCHEMA_WITHOUT_NAME_CONFLICTS)
            .caseSensitive(false)
            .bucket("data", 10, "partition1")
            .bucket("DATA", 10, "PARTITION1")
            .build();

    StructType expectedType =
        StructType.of(
            NestedField.optional(1000, "partition1", Types.IntegerType.get()),
            NestedField.optional(1001, "PARTITION1", Types.IntegerType.get()));
    StructType actualType = spec.partitionType();
    assertThat(actualType).isEqualTo(expectedType);
  }

  @Test
  public void testBucketTargetNameAllowsInexactDuplicateWhenCaseInsensitive() {
    PartitionSpec spec =
        PartitionSpec.builderFor(SCHEMA_WITHOUT_NAME_CONFLICTS)
            .caseSensitive(false)
            .bucket("data", 10, "partition1")
            .bucket("category", 10, "PARTITION1")
            .build();

    StructType expectedType =
        StructType.of(
            NestedField.optional(1000, "partition1", Types.IntegerType.get()),
            NestedField.optional(1001, "PARTITION1", Types.IntegerType.get()));
    StructType actualType = spec.partitionType();
    assertThat(actualType).isEqualTo(expectedType);
  }

  @Test
  public void testBucketTargetNameDoesNotAllowExactDuplicateWhenCaseInsensitive() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                PartitionSpec.builderFor(SCHEMA_WITHOUT_NAME_CONFLICTS)
                    .caseSensitive(false)
                    .bucket("data", 10, "partition1")
                    .bucket("category", 10, "partition1")
                    .build())
        .withMessage("Cannot use partition name more than once: partition1");
  }

  @Test
  public void testBucketTargetNameDoesNotAllowExactDuplicateWhenCaseSensitive() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                PartitionSpec.builderFor(SCHEMA_WITH_NAME_CONFLICTS)
                    .bucket("data", 10, "partition1")
                    .bucket("DATA", 10, "partition1")
                    .build())
        .withMessage("Cannot use partition name more than once: partition1");
  }

  @Test
  public void testTruncateTargetNameDefaultValue() {
    PartitionSpec spec =
        PartitionSpec.builderFor(SCHEMA_WITH_NAME_CONFLICTS).truncate("data", 10).build();

    StructType expectedType =
        StructType.of(NestedField.optional(1000, "data_trunc", Types.StringType.get()));
    StructType actualType = spec.partitionType();
    assertThat(actualType).isEqualTo(expectedType);
  }

  @Test
  public void testTruncateTargetNameDefaultValueCaseInsensitive() {
    PartitionSpec spec =
        PartitionSpec.builderFor(SCHEMA_WITHOUT_NAME_CONFLICTS)
            .caseSensitive(false)
            .truncate("DATA", 10)
            .build();

    StructType expectedType =
        StructType.of(NestedField.optional(1000, "data_trunc", Types.StringType.get()));
    StructType actualType = spec.partitionType();
    assertThat(actualType).isEqualTo(expectedType);
  }

  @Test
  public void testTruncateSourceNameAllowsExactDuplicateWhenCaseSensitive() {
    PartitionSpec spec =
        PartitionSpec.builderFor(SCHEMA_WITH_NAME_CONFLICTS)
            .truncate("data", 10, "partition1")
            .truncate("data", 10, "PARTITION1")
            .build();

    StructType expectedType =
        StructType.of(
            NestedField.optional(1000, "partition1", Types.StringType.get()),
            NestedField.optional(1001, "PARTITION1", Types.StringType.get()));
    StructType actualType = spec.partitionType();
    assertThat(actualType).isEqualTo(expectedType);
  }

  @Test
  public void testTruncateSourceNameAllowsInexactDuplicateWhenCaseInsensitive() {
    PartitionSpec spec =
        PartitionSpec.builderFor(SCHEMA_WITHOUT_NAME_CONFLICTS)
            .caseSensitive(false)
            .truncate("data", 10, "partition1")
            .truncate("DATA", 10, "PARTITION1")
            .build();

    StructType expectedType =
        StructType.of(
            NestedField.optional(1000, "partition1", Types.StringType.get()),
            NestedField.optional(1001, "PARTITION1", Types.StringType.get()));
    StructType actualType = spec.partitionType();
    assertThat(actualType).isEqualTo(expectedType);
  }

  @Test
  public void testTruncateTargetNameAllowsInexactDuplicateWhenCaseInsensitive() {
    PartitionSpec spec =
        PartitionSpec.builderFor(SCHEMA_WITHOUT_NAME_CONFLICTS)
            .caseSensitive(false)
            .truncate("data", 10, "partition1")
            .truncate("category", 10, "PARTITION1")
            .build();

    StructType expectedType =
        StructType.of(
            NestedField.optional(1000, "partition1", Types.StringType.get()),
            NestedField.optional(1001, "PARTITION1", Types.StringType.get()));
    StructType actualType = spec.partitionType();
    assertThat(actualType).isEqualTo(expectedType);
  }

  @Test
  public void testTruncateTargetNameDoesNotAllowExactDuplicateWhenCaseInsensitive() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                PartitionSpec.builderFor(SCHEMA_WITHOUT_NAME_CONFLICTS)
                    .caseSensitive(false)
                    .truncate("data", 10, "partition1")
                    .truncate("category", 10, "partition1")
                    .build())
        .withMessage("Cannot use partition name more than once: partition1");
  }

  @Test
  public void testTruncateTargetNameDoesNotAllowExactDuplicateWhenCaseSensitive() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                PartitionSpec.builderFor(SCHEMA_WITH_NAME_CONFLICTS)
                    .truncate("data", 10, "partition1")
                    .truncate("DATA", 10, "partition1")
                    .build())
        .withMessage("Cannot use partition name more than once: partition1");
  }

  @Test
  public void testIdentityTargetNameDefaultValue() {
    PartitionSpec spec =
        PartitionSpec.builderFor(SCHEMA_WITH_NAME_CONFLICTS).identity("data").build();

    StructType expectedType =
        StructType.of(NestedField.optional(1000, "data", Types.StringType.get()));
    StructType actualType = spec.partitionType();
    assertThat(actualType).isEqualTo(expectedType);
  }

  @Test
  public void testIdentityTargetNameDefaultValueCaseInsensitive() {
    PartitionSpec spec =
        PartitionSpec.builderFor(SCHEMA_WITHOUT_NAME_CONFLICTS)
            .caseSensitive(false)
            .identity("DATA")
            .build();

    StructType expectedType =
        StructType.of(NestedField.optional(1000, "data", Types.StringType.get()));
    StructType actualType = spec.partitionType();
    assertThat(actualType).isEqualTo(expectedType);
  }

  @Test
  public void testIdentitySourceNameDoesNotAllowExactDuplicateWhenCaseSensitive() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                PartitionSpec.builderFor(SCHEMA_WITH_NAME_CONFLICTS)
                    .identity("data", "partition1")
                    .identity("data", "PARTITION1")
                    .build())
        .withMessage(
            "Cannot add redundant partition: 1000: partition1: identity(2) conflicts with 1001: PARTITION1: identity(2)");
  }

  @Test
  public void testIdentitySourceNameDoesNotAllowInexactDuplicateWhenCaseInsensitive() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                PartitionSpec.builderFor(SCHEMA_WITHOUT_NAME_CONFLICTS)
                    .caseSensitive(false)
                    .identity("data", "partition1")
                    .identity("DATA", "PARTITION1")
                    .build())
        .withMessage(
            "Cannot add redundant partition: 1000: partition1: identity(2) conflicts with 1001: PARTITION1: identity(2)");
  }

  @Test
  public void testIdentityTargetNameAllowsInexactDuplicateWhenCaseInsensitive() {
    PartitionSpec spec =
        PartitionSpec.builderFor(SCHEMA_WITHOUT_NAME_CONFLICTS)
            .caseSensitive(false)
            .identity("data", "partition1")
            .identity("category", "PARTITION1")
            .build();

    StructType expectedType =
        StructType.of(
            NestedField.optional(1000, "partition1", Types.StringType.get()),
            NestedField.optional(1001, "PARTITION1", Types.StringType.get()));
    StructType actualType = spec.partitionType();
    assertThat(actualType).isEqualTo(expectedType);
  }

  @Test
  public void testIdentityTargetNameDoesNotAllowExactDuplicateWhenCaseInsensitive() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                PartitionSpec.builderFor(SCHEMA_WITHOUT_NAME_CONFLICTS)
                    .caseSensitive(false)
                    .identity("data", "partition1")
                    .identity("category", "partition1")
                    .build())
        .withMessage("Cannot use partition name more than once: partition1");
  }

  @Test
  public void testIdentityTargetNameDoesNotAllowExactDuplicateWhenCaseSensitive() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                PartitionSpec.builderFor(SCHEMA_WITH_NAME_CONFLICTS)
                    .identity("data", "partition1")
                    .identity("DATA", "partition1")
                    .build())
        .withMessage("Cannot use partition name more than once: partition1");
  }

  @Test
  public void testAlwaysNullTargetNameDefaultValue() {
    PartitionSpec spec =
        PartitionSpec.builderFor(SCHEMA_WITH_NAME_CONFLICTS).alwaysNull("data").build();

    StructType expectedType =
        StructType.of(NestedField.optional(1000, "data_null", Types.StringType.get()));
    StructType actualType = spec.partitionType();
    assertThat(actualType).isEqualTo(expectedType);
  }

  @Test
  public void testAlwaysNullTargetNameDefaultValueCaseInsensitive() {
    PartitionSpec spec =
        PartitionSpec.builderFor(SCHEMA_WITHOUT_NAME_CONFLICTS)
            .caseSensitive(false)
            .alwaysNull("DATA")
            .build();

    StructType expectedType =
        StructType.of(NestedField.optional(1000, "data_null", Types.StringType.get()));
    StructType actualType = spec.partitionType();
    assertThat(actualType).isEqualTo(expectedType);
  }

  @Test
  public void testAlwaysNullSourceNameAllowsExactDuplicateWhenCaseSensitive() {
    PartitionSpec spec =
        PartitionSpec.builderFor(SCHEMA_WITH_NAME_CONFLICTS)
            .alwaysNull("data", "partition1")
            .alwaysNull("data", "PARTITION1")
            .build();

    StructType expectedType =
        StructType.of(
            NestedField.optional(1000, "partition1", Types.StringType.get()),
            NestedField.optional(1001, "PARTITION1", Types.StringType.get()));
    StructType actualType = spec.partitionType();
    assertThat(actualType).isEqualTo(expectedType);
  }

  @Test
  public void testAlwaysNullSourceNameAllowsInexactDuplicateWhenCaseInsensitive() {
    PartitionSpec spec =
        PartitionSpec.builderFor(SCHEMA_WITHOUT_NAME_CONFLICTS)
            .caseSensitive(false)
            .alwaysNull("data", "partition1")
            .alwaysNull("DATA", "PARTITION1")
            .build();

    StructType expectedType =
        StructType.of(
            NestedField.optional(1000, "partition1", Types.StringType.get()),
            NestedField.optional(1001, "PARTITION1", Types.StringType.get()));
    StructType actualType = spec.partitionType();
    assertThat(actualType).isEqualTo(expectedType);
  }

  @Test
  public void testAlwaysNullTargetNameAllowsInexactDuplicateWhenCaseInsensitive() {
    PartitionSpec spec =
        PartitionSpec.builderFor(SCHEMA_WITHOUT_NAME_CONFLICTS)
            .caseSensitive(false)
            .alwaysNull("data", "partition1")
            .alwaysNull("category", "PARTITION1")
            .build();

    StructType expectedType =
        StructType.of(
            NestedField.optional(1000, "partition1", Types.StringType.get()),
            NestedField.optional(1001, "PARTITION1", Types.StringType.get()));
    StructType actualType = spec.partitionType();
    assertThat(actualType).isEqualTo(expectedType);
  }

  @Test
  public void testAlwaysNullTargetNameDoesNotAllowExactDuplicateWhenCaseInsensitive() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                PartitionSpec.builderFor(SCHEMA_WITHOUT_NAME_CONFLICTS)
                    .caseSensitive(false)
                    .alwaysNull("data", "partition1")
                    .alwaysNull("category", "partition1")
                    .build())
        .withMessage("Cannot use partition name more than once: partition1");
  }

  @Test
  public void testAlwaysNullTargetNameDoesNotAllowExactDuplicateWhenCaseSensitive() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                PartitionSpec.builderFor(SCHEMA_WITH_NAME_CONFLICTS)
                    .alwaysNull("data", "partition1")
                    .alwaysNull("DATA", "partition1")
                    .build())
        .withMessage("Cannot use partition name more than once: partition1");
  }

  @Test
  public void testYearTargetNameDefaultValue() {
    PartitionSpec spec =
        PartitionSpec.builderFor(SCHEMA_WITH_NAME_CONFLICTS).year("order_date").build();

    StructType expectedType =
        StructType.of(NestedField.optional(1000, "order_date_year", Types.IntegerType.get()));
    StructType actualType = spec.partitionType();
    assertThat(actualType).isEqualTo(expectedType);
  }

  @Test
  public void testYearTargetNameDefaultValueCaseInsensitive() {
    PartitionSpec spec =
        PartitionSpec.builderFor(SCHEMA_WITHOUT_NAME_CONFLICTS)
            .caseSensitive(false)
            .year("ORDER_DATE")
            .build();

    StructType expectedType =
        StructType.of(NestedField.optional(1000, "order_date_year", Types.IntegerType.get()));
    StructType actualType = spec.partitionType();
    assertThat(actualType).isEqualTo(expectedType);
  }

  @Test
  public void testYearSourceNameDoesNotAllowExactDuplicateWhenCaseSensitive() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                PartitionSpec.builderFor(SCHEMA_WITH_NAME_CONFLICTS)
                    .year("order_date", "partition1")
                    .year("order_date", "PARTITION1")
                    .build())
        .withMessage(
            "Cannot add redundant partition: 1000: partition1: year(4) conflicts with 1001: PARTITION1: year(4)");
  }

  @Test
  public void testYearSourceNameDoesNotAllowInexactDuplicateWhenCaseInsensitive() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                PartitionSpec.builderFor(SCHEMA_WITHOUT_NAME_CONFLICTS)
                    .caseSensitive(false)
                    .year("order_date", "partition1")
                    .year("ORDER_DATE", "PARTITION1")
                    .build())
        .withMessage(
            "Cannot add redundant partition: 1000: partition1: year(4) conflicts with 1001: PARTITION1: year(4)");
  }

  @Test
  public void testYearTargetNameAllowsInexactDuplicateWhenCaseInsensitive() {
    PartitionSpec spec =
        PartitionSpec.builderFor(SCHEMA_WITHOUT_NAME_CONFLICTS)
            .caseSensitive(false)
            .year("order_date", "partition1")
            .year("ship_date", "PARTITION1")
            .build();

    StructType expectedType =
        StructType.of(
            NestedField.optional(1000, "partition1", Types.IntegerType.get()),
            NestedField.optional(1001, "PARTITION1", Types.IntegerType.get()));
    StructType actualType = spec.partitionType();
    assertThat(actualType).isEqualTo(expectedType);
  }

  @Test
  public void testYearTargetNameDoesNotAllowExactDuplicateWhenCaseInsensitive() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                PartitionSpec.builderFor(SCHEMA_WITHOUT_NAME_CONFLICTS)
                    .caseSensitive(false)
                    .year("order_date", "partition1")
                    .year("ship_date", "partition1")
                    .build())
        .withMessage("Cannot use partition name more than once: partition1");
  }

  @Test
  public void testYearTargetNameDoesNotAllowExactDuplicateWhenCaseSensitive() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                PartitionSpec.builderFor(SCHEMA_WITH_NAME_CONFLICTS)
                    .year("order_date", "partition1")
                    .year("ORDER_DATE", "partition1")
                    .build())
        .withMessage("Cannot use partition name more than once: partition1");
  }

  @Test
  public void testMonthTargetNameDefaultValue() {
    PartitionSpec spec =
        PartitionSpec.builderFor(SCHEMA_WITH_NAME_CONFLICTS).month("order_date").build();

    StructType expectedType =
        StructType.of(NestedField.optional(1000, "order_date_month", Types.IntegerType.get()));
    StructType actualType = spec.partitionType();
    assertThat(actualType).isEqualTo(expectedType);
  }

  @Test
  public void testMonthTargetNameDefaultValueCaseInsensitive() {
    PartitionSpec spec =
        PartitionSpec.builderFor(SCHEMA_WITHOUT_NAME_CONFLICTS)
            .caseSensitive(false)
            .month("ORDER_DATE")
            .build();

    StructType expectedType =
        StructType.of(NestedField.optional(1000, "order_date_month", Types.IntegerType.get()));
    StructType actualType = spec.partitionType();
    assertThat(actualType).isEqualTo(expectedType);
  }

  @Test
  public void testMonthSourceNameDoesNotAllowExactDuplicateWhenCaseSensitive() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                PartitionSpec.builderFor(SCHEMA_WITH_NAME_CONFLICTS)
                    .month("order_date", "partition1")
                    .month("order_date", "PARTITION1")
                    .build())
        .withMessage(
            "Cannot add redundant partition: 1000: partition1: month(4) conflicts with 1001: PARTITION1: month(4)");
  }

  @Test
  public void testMonthSourceNameDoesNotAllowInexactDuplicateWhenCaseInsensitive() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                PartitionSpec.builderFor(SCHEMA_WITHOUT_NAME_CONFLICTS)
                    .caseSensitive(false)
                    .month("order_date", "partition1")
                    .month("ORDER_DATE", "PARTITION1")
                    .build())
        .withMessage(
            "Cannot add redundant partition: 1000: partition1: month(4) conflicts with 1001: PARTITION1: month(4)");
  }

  @Test
  public void testMonthTargetNameAllowsInexactDuplicateWhenCaseInsensitive() {
    PartitionSpec spec =
        PartitionSpec.builderFor(SCHEMA_WITHOUT_NAME_CONFLICTS)
            .caseSensitive(false)
            .month("order_date", "partition1")
            .month("ship_date", "PARTITION1")
            .build();

    StructType expectedType =
        StructType.of(
            NestedField.optional(1000, "partition1", Types.IntegerType.get()),
            NestedField.optional(1001, "PARTITION1", Types.IntegerType.get()));
    StructType actualType = spec.partitionType();
    assertThat(actualType).isEqualTo(expectedType);
  }

  @Test
  public void testMonthTargetNameDoesNotAllowExactDuplicateWhenCaseInsensitive() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                PartitionSpec.builderFor(SCHEMA_WITHOUT_NAME_CONFLICTS)
                    .caseSensitive(false)
                    .month("order_date", "partition1")
                    .month("ship_date", "partition1")
                    .build())
        .withMessage("Cannot use partition name more than once: partition1");
  }

  @Test
  public void testMonthTargetNameDoesNotAllowExactDuplicateWhenCaseSensitive() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                PartitionSpec.builderFor(SCHEMA_WITH_NAME_CONFLICTS)
                    .month("order_date", "partition1")
                    .month("ORDER_DATE", "partition1")
                    .build())
        .withMessage("Cannot use partition name more than once: partition1");
  }

  @Test
  public void testDayTargetNameDefaultValue() {
    PartitionSpec spec =
        PartitionSpec.builderFor(SCHEMA_WITH_NAME_CONFLICTS).day("order_date").build();

    StructType expectedType =
        StructType.of(NestedField.optional(1000, "order_date_day", Types.DateType.get()));
    StructType actualType = spec.partitionType();
    assertThat(actualType).isEqualTo(expectedType);
  }

  @Test
  public void testDayTargetNameDefaultValueCaseInsensitive() {
    PartitionSpec spec =
        PartitionSpec.builderFor(SCHEMA_WITHOUT_NAME_CONFLICTS)
            .caseSensitive(false)
            .day("ORDER_DATE")
            .build();

    StructType expectedType =
        StructType.of(NestedField.optional(1000, "order_date_day", Types.DateType.get()));
    StructType actualType = spec.partitionType();
    assertThat(actualType).isEqualTo(expectedType);
  }

  @Test
  public void testDaySourceNameDoesNotAllowExactDuplicateWhenCaseSensitive() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                PartitionSpec.builderFor(SCHEMA_WITH_NAME_CONFLICTS)
                    .day("order_date", "partition1")
                    .day("order_date", "PARTITION1")
                    .build())
        .withMessage(
            "Cannot add redundant partition: 1000: partition1: day(4) conflicts with 1001: PARTITION1: day(4)");
  }

  @Test
  public void testDaySourceNameDoesNotAllowInexactDuplicateWhenCaseInsensitive() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                PartitionSpec.builderFor(SCHEMA_WITHOUT_NAME_CONFLICTS)
                    .caseSensitive(false)
                    .day("order_date", "partition1")
                    .day("ORDER_DATE", "PARTITION1")
                    .build())
        .withMessage(
            "Cannot add redundant partition: 1000: partition1: day(4) conflicts with 1001: PARTITION1: day(4)");
  }

  @Test
  public void testDayTargetNameAllowsInexactDuplicateWhenCaseInsensitive() {
    PartitionSpec spec =
        PartitionSpec.builderFor(SCHEMA_WITHOUT_NAME_CONFLICTS)
            .caseSensitive(false)
            .day("order_date", "partition1")
            .day("ship_date", "PARTITION1")
            .build();

    StructType expectedType =
        StructType.of(
            NestedField.optional(1000, "partition1", Types.DateType.get()),
            NestedField.optional(1001, "PARTITION1", Types.DateType.get()));
    StructType actualType = spec.partitionType();
    assertThat(actualType).isEqualTo(expectedType);
  }

  @Test
  public void testDayTargetNameDoesNotAllowExactDuplicateWhenCaseInsensitive() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                PartitionSpec.builderFor(SCHEMA_WITHOUT_NAME_CONFLICTS)
                    .caseSensitive(false)
                    .day("order_date", "partition1")
                    .day("ship_date", "partition1")
                    .build())
        .withMessage("Cannot use partition name more than once: partition1");
  }

  @Test
  public void testDayTargetNameDoesNotAllowExactDuplicateWhenCaseSensitive() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                PartitionSpec.builderFor(SCHEMA_WITH_NAME_CONFLICTS)
                    .day("order_date", "partition1")
                    .day("ORDER_DATE", "partition1")
                    .build())
        .withMessage("Cannot use partition name more than once: partition1");
  }

  @Test
  public void testHourTargetNameDefaultValue() {
    PartitionSpec spec =
        PartitionSpec.builderFor(SCHEMA_WITH_NAME_CONFLICTS).hour("order_time").build();

    StructType expectedType =
        StructType.of(NestedField.optional(1000, "order_time_hour", Types.IntegerType.get()));
    StructType actualType = spec.partitionType();
    assertThat(actualType).isEqualTo(expectedType);
  }

  @Test
  public void testHourTargetNameDefaultValueCaseInsensitive() {
    PartitionSpec spec =
        PartitionSpec.builderFor(SCHEMA_WITHOUT_NAME_CONFLICTS)
            .caseSensitive(false)
            .hour("ORDER_TIME")
            .build();

    StructType expectedType =
        StructType.of(NestedField.optional(1000, "order_time_hour", Types.IntegerType.get()));
    StructType actualType = spec.partitionType();
    assertThat(actualType).isEqualTo(expectedType);
  }

  @Test
  public void testHourSourceNameDoesNotAllowExactDuplicateWhenCaseSensitive() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                PartitionSpec.builderFor(SCHEMA_WITH_NAME_CONFLICTS)
                    .hour("order_time", "partition1")
                    .hour("order_time", "PARTITION1")
                    .build())
        .withMessage(
            "Cannot add redundant partition: 1000: partition1: hour(6) conflicts with 1001: PARTITION1: hour(6)");
  }

  @Test
  public void testHourSourceNameDoesNotAllowInexactDuplicateWhenCaseInsensitive() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                PartitionSpec.builderFor(SCHEMA_WITHOUT_NAME_CONFLICTS)
                    .caseSensitive(false)
                    .hour("order_time", "partition1")
                    .hour("ORDER_TIME", "PARTITION1")
                    .build())
        .withMessage(
            "Cannot add redundant partition: 1000: partition1: hour(5) conflicts with 1001: PARTITION1: hour(5)");
  }

  @Test
  public void testHourTargetNameAllowsInexactDuplicateWhenCaseInsensitive() {
    PartitionSpec spec =
        PartitionSpec.builderFor(SCHEMA_WITHOUT_NAME_CONFLICTS)
            .caseSensitive(false)
            .hour("order_time", "partition1")
            .hour("ship_time", "PARTITION1")
            .build();

    StructType expectedType =
        StructType.of(
            NestedField.optional(1000, "partition1", Types.IntegerType.get()),
            NestedField.optional(1001, "PARTITION1", Types.IntegerType.get()));
    StructType actualType = spec.partitionType();
    assertThat(actualType).isEqualTo(expectedType);
  }

  @Test
  public void testHourTargetNameDoesNotAllowExactDuplicateWhenCaseInsensitive() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                PartitionSpec.builderFor(SCHEMA_WITHOUT_NAME_CONFLICTS)
                    .caseSensitive(false)
                    .hour("order_time", "partition1")
                    .hour("ship_time", "partition1")
                    .build())
        .withMessage("Cannot use partition name more than once: partition1");
  }

  @Test
  public void testHourTargetNameDoesNotAllowExactDuplicateWhenCaseSensitive() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                PartitionSpec.builderFor(SCHEMA_WITH_NAME_CONFLICTS)
                    .hour("order_time", "partition1")
                    .hour("ORDER_TIME", "partition1")
                    .build())
        .withMessage("Cannot use partition name more than once: partition1");
  }
}
