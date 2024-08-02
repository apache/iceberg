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
  private static final Schema SCHEMA_CASE_INSENSITIVE =
      new Schema(
          required(1, "id", Types.IntegerType.get()),
          required(2, "data", Types.StringType.get()),
          required(3, "category", Types.StringType.get()),
          required(4, "order_date", Types.DateType.get()),
          required(5, "order_time", Types.TimestampType.withoutZone()),
          required(6, "ship_date", Types.DateType.get()),
          required(7, "ship_time", Types.TimestampType.withoutZone()));

  private static final Schema SCHEMA_CASE_SENSITIVE =
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
    TestTables.TestTable table =
        TestTables.create(tableDir, "test", schema, spec, V2_FORMAT_VERSION);

    StructType expectedType =
        StructType.of(
            NestedField.optional(1000, "data", Types.StringType.get()),
            NestedField.optional(1001, "DATA", Types.StringType.get()));
    StructType actualType = Partitioning.partitionType(table);
    assertThat(actualType).isEqualTo(expectedType);
  }

  @Test
  public void testPartitionTypeWithIdentityTargetName() {
    PartitionSpec spec =
        PartitionSpec.builderFor(SCHEMA_CASE_INSENSITIVE).identity("data", "p1").build();
    TestTables.TestTable table =
        TestTables.create(tableDir, "test", SCHEMA_CASE_INSENSITIVE, spec, V2_FORMAT_VERSION);

    StructType expectedType =
        StructType.of(NestedField.optional(1000, "p1", Types.StringType.get()));
    StructType actualType = Partitioning.partitionType(table);
    assertThat(actualType).isEqualTo(expectedType);
  }

  @Test
  public void testBucketSourceNameAllowsExactDuplicateWhenCaseSensitive() {
    Schema schema = SCHEMA_CASE_SENSITIVE;
    PartitionSpec.Builder builder =
        PartitionSpec.builderFor(schema).bucket("data", 10, "p1").bucket("data", 10, "P1");

    StructType expectedType =
        StructType.of(
            NestedField.optional(1000, "p1", Types.IntegerType.get()),
            NestedField.optional(1001, "P1", Types.IntegerType.get()));
    PartitionSpec spec = builder.build();

    TestTables.TestTable table =
        TestTables.create(tableDir, "test", schema, spec, V2_FORMAT_VERSION);

    StructType actualType = Partitioning.partitionType(table);
    assertThat(actualType).isEqualTo(expectedType);
  }

  @Test
  public void testBucketSourceNameAllowsInexactDuplicateWhenCaseInsensitive() {
    Schema schema = SCHEMA_CASE_INSENSITIVE;
    PartitionSpec.Builder builder =
        PartitionSpec.builderFor(schema)
            .caseSensitive(false)
            .bucket("data", 10, "p1")
            .bucket("DATA", 10, "P1");

    StructType expectedType =
        StructType.of(
            NestedField.optional(1000, "p1", Types.IntegerType.get()),
            NestedField.optional(1001, "P1", Types.IntegerType.get()));
    PartitionSpec spec = builder.build();

    TestTables.TestTable table =
        TestTables.create(tableDir, "test", schema, spec, V2_FORMAT_VERSION);

    StructType actualType = Partitioning.partitionType(table);
    assertThat(actualType).isEqualTo(expectedType);
  }

  @Test
  public void testBucketTargetNameAllowsInexactDuplicateWhenCaseInsensitive() {
    Schema schema = SCHEMA_CASE_INSENSITIVE;
    PartitionSpec.Builder builder =
        PartitionSpec.builderFor(schema)
            .caseSensitive(false)
            .bucket("data", 10, "p1")
            .bucket("category", 10, "P1");

    StructType expectedType =
        StructType.of(
            NestedField.optional(1000, "p1", Types.IntegerType.get()),
            NestedField.optional(1001, "P1", Types.IntegerType.get()));
    PartitionSpec spec = builder.build();

    TestTables.TestTable table =
        TestTables.create(tableDir, "test", schema, spec, V2_FORMAT_VERSION);

    StructType actualType = Partitioning.partitionType(table);
    assertThat(actualType).isEqualTo(expectedType);
  }

  @Test
  public void testBucketTargetNameDoesNotAllowExactDuplicateWhenCaseInsensitive() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                PartitionSpec.builderFor(SCHEMA_CASE_INSENSITIVE)
                    .caseSensitive(false)
                    .bucket("data", 10, "p1")
                    .bucket("category", 10, "p1")
                    .build())
        .withMessage("Cannot use partition name more than once: p1");
  }

  @Test
  public void testBucketTargetNameDoesNotAllowExactDuplicateWhenCaseSensitive() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                PartitionSpec.builderFor(SCHEMA_CASE_SENSITIVE)
                    .bucket("data", 10, "p1")
                    .bucket("DATA", 10, "p1")
                    .build())
        .withMessage("Cannot use partition name more than once: p1");
  }

  @Test
  public void testTruncateSourceNameAllowsExactDuplicateWhenCaseSensitive() {
    Schema schema = SCHEMA_CASE_SENSITIVE;
    PartitionSpec.Builder builder =
        PartitionSpec.builderFor(schema).truncate("data", 10, "p1").truncate("data", 10, "P1");

    StructType expectedType =
        StructType.of(
            NestedField.optional(1000, "p1", Types.StringType.get()),
            NestedField.optional(1001, "P1", Types.StringType.get()));
    PartitionSpec spec = builder.build();

    TestTables.TestTable table =
        TestTables.create(tableDir, "test", schema, spec, V2_FORMAT_VERSION);

    StructType actualType = Partitioning.partitionType(table);
    assertThat(actualType).isEqualTo(expectedType);
  }

  @Test
  public void testTruncateSourceNameAllowsInexactDuplicateWhenCaseInsensitive() {
    Schema schema = SCHEMA_CASE_INSENSITIVE;
    PartitionSpec.Builder builder =
        PartitionSpec.builderFor(schema)
            .caseSensitive(false)
            .truncate("data", 10, "p1")
            .truncate("DATA", 10, "P1");

    StructType expectedType =
        StructType.of(
            NestedField.optional(1000, "p1", Types.StringType.get()),
            NestedField.optional(1001, "P1", Types.StringType.get()));
    PartitionSpec spec = builder.build();

    TestTables.TestTable table =
        TestTables.create(tableDir, "test", schema, spec, V2_FORMAT_VERSION);

    StructType actualType = Partitioning.partitionType(table);
    assertThat(actualType).isEqualTo(expectedType);
  }

  @Test
  public void testTruncateTargetNameAllowsInexactDuplicateWhenCaseInsensitive() {
    Schema schema = SCHEMA_CASE_INSENSITIVE;
    PartitionSpec.Builder builder =
        PartitionSpec.builderFor(schema)
            .caseSensitive(false)
            .truncate("data", 10, "p1")
            .truncate("category", 10, "P1");

    StructType expectedType =
        StructType.of(
            NestedField.optional(1000, "p1", Types.StringType.get()),
            NestedField.optional(1001, "P1", Types.StringType.get()));
    PartitionSpec spec = builder.build();

    TestTables.TestTable table =
        TestTables.create(tableDir, "test", schema, spec, V2_FORMAT_VERSION);

    StructType actualType = Partitioning.partitionType(table);
    assertThat(actualType).isEqualTo(expectedType);
  }

  @Test
  public void testTruncateTargetNameDoesNotAllowExactDuplicateWhenCaseInsensitive() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                PartitionSpec.builderFor(SCHEMA_CASE_INSENSITIVE)
                    .caseSensitive(false)
                    .truncate("data", 10, "p1")
                    .truncate("category", 10, "p1")
                    .build())
        .withMessage("Cannot use partition name more than once: p1");
  }

  @Test
  public void testTruncateTargetNameDoesNotAllowExactDuplicateWhenCaseSensitive() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                PartitionSpec.builderFor(SCHEMA_CASE_SENSITIVE)
                    .truncate("data", 10, "p1")
                    .truncate("DATA", 10, "p1")
                    .build())
        .withMessage("Cannot use partition name more than once: p1");
  }

  @Test
  public void testIdentitySourceNameAllowsExactDuplicateWhenCaseSensitive() {
    Schema schema = SCHEMA_CASE_SENSITIVE;
    PartitionSpec.Builder builder =
        PartitionSpec.builderFor(schema).identity("data", "p1").identity("data", "P1");

    StructType expectedType =
        StructType.of(
            NestedField.optional(1000, "p1", Types.StringType.get()),
            NestedField.optional(1001, "P1", Types.StringType.get()));
    PartitionSpec spec = builder.build();

    TestTables.TestTable table =
        TestTables.create(tableDir, "test", schema, spec, V2_FORMAT_VERSION);

    StructType actualType = Partitioning.partitionType(table);
    assertThat(actualType).isEqualTo(expectedType);
  }

  @Test
  public void testIdentitySourceNameAllowsInexactDuplicateWhenCaseInsensitive() {
    Schema schema = SCHEMA_CASE_INSENSITIVE;
    PartitionSpec.Builder builder =
        PartitionSpec.builderFor(schema)
            .caseSensitive(false)
            .identity("data", "p1")
            .identity("DATA", "P1");

    StructType expectedType =
        StructType.of(
            NestedField.optional(1000, "p1", Types.StringType.get()),
            NestedField.optional(1001, "P1", Types.StringType.get()));
    PartitionSpec spec = builder.build();

    TestTables.TestTable table =
        TestTables.create(tableDir, "test", schema, spec, V2_FORMAT_VERSION);

    StructType actualType = Partitioning.partitionType(table);
    assertThat(actualType).isEqualTo(expectedType);
  }

  @Test
  public void testIdentityTargetNameAllowsInexactDuplicateWhenCaseInsensitive() {
    Schema schema = SCHEMA_CASE_INSENSITIVE;
    PartitionSpec.Builder builder =
        PartitionSpec.builderFor(schema)
            .caseSensitive(false)
            .identity("data", "p1")
            .identity("category", "P1");

    StructType expectedType =
        StructType.of(
            NestedField.optional(1000, "p1", Types.StringType.get()),
            NestedField.optional(1001, "P1", Types.StringType.get()));
    PartitionSpec spec = builder.build();

    TestTables.TestTable table =
        TestTables.create(tableDir, "test", schema, spec, V2_FORMAT_VERSION);

    StructType actualType = Partitioning.partitionType(table);
    assertThat(actualType).isEqualTo(expectedType);
  }

  @Test
  public void testIdentityTargetNameDoesNotAllowExactDuplicateWhenCaseInsensitive() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                PartitionSpec.builderFor(SCHEMA_CASE_INSENSITIVE)
                    .caseSensitive(false)
                    .identity("data", "p1")
                    .identity("category", "p1")
                    .build())
        .withMessage("Cannot use partition name more than once: p1");
  }

  @Test
  public void testIdentityTargetNameDoesNotAllowExactDuplicateWhenCaseSensitive() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                PartitionSpec.builderFor(SCHEMA_CASE_SENSITIVE)
                    .identity("data", "p1")
                    .identity("DATA", "p1")
                    .build())
        .withMessage("Cannot use partition name more than once: p1");
  }

  @Test
  public void testAlwaysNullSourceNameAllowsExactDuplicateWhenCaseSensitive() {
    Schema schema = SCHEMA_CASE_SENSITIVE;
    PartitionSpec.Builder builder =
        PartitionSpec.builderFor(schema).alwaysNull("data", "p1").alwaysNull("data", "P1");

    StructType expectedType =
        StructType.of(
            NestedField.optional(1000, "p1", Types.StringType.get()),
            NestedField.optional(1001, "P1", Types.StringType.get()));
    PartitionSpec spec = builder.build();

    TestTables.TestTable table =
        TestTables.create(tableDir, "test", schema, spec, V2_FORMAT_VERSION);

    StructType actualType = Partitioning.partitionType(table);
    assertThat(actualType).isEqualTo(expectedType);
  }

  @Test
  public void testAlwaysNullSourceNameAllowsInexactDuplicateWhenCaseInsensitive() {
    Schema schema = SCHEMA_CASE_INSENSITIVE;
    PartitionSpec.Builder builder =
        PartitionSpec.builderFor(schema)
            .caseSensitive(false)
            .alwaysNull("data", "p1")
            .alwaysNull("DATA", "P1");

    StructType expectedType =
        StructType.of(
            NestedField.optional(1000, "p1", Types.StringType.get()),
            NestedField.optional(1001, "P1", Types.StringType.get()));
    PartitionSpec spec = builder.build();

    TestTables.TestTable table =
        TestTables.create(tableDir, "test", schema, spec, V2_FORMAT_VERSION);

    StructType actualType = Partitioning.partitionType(table);
    assertThat(actualType).isEqualTo(expectedType);
  }

  @Test
  public void testAlwaysNullTargetNameAllowsInexactDuplicateWhenCaseInsensitive() {
    Schema schema = SCHEMA_CASE_INSENSITIVE;
    PartitionSpec.Builder builder =
        PartitionSpec.builderFor(schema)
            .caseSensitive(false)
            .alwaysNull("data", "p1")
            .alwaysNull("category", "P1");

    StructType expectedType =
        StructType.of(
            NestedField.optional(1000, "p1", Types.StringType.get()),
            NestedField.optional(1001, "P1", Types.StringType.get()));
    PartitionSpec spec = builder.build();

    TestTables.TestTable table =
        TestTables.create(tableDir, "test", schema, spec, V2_FORMAT_VERSION);

    StructType actualType = Partitioning.partitionType(table);
    assertThat(actualType).isEqualTo(expectedType);
  }

  @Test
  public void testAlwaysNullTargetNameDoesNotAllowExactDuplicateWhenCaseInsensitive() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                PartitionSpec.builderFor(SCHEMA_CASE_INSENSITIVE)
                    .caseSensitive(false)
                    .alwaysNull("data", "p1")
                    .alwaysNull("category", "p1")
                    .build())
        .withMessage("Cannot use partition name more than once: p1");
  }

  @Test
  public void testAlwaysNullTargetNameDoesNotAllowExactDuplicateWhenCaseSensitive() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                PartitionSpec.builderFor(SCHEMA_CASE_SENSITIVE)
                    .alwaysNull("data", "p1")
                    .alwaysNull("DATA", "p1")
                    .build())
        .withMessage("Cannot use partition name more than once: p1");
  }

  @Test
  public void testYearSourceNameAllowsExactDuplicateWhenCaseSensitive() {
    Schema schema = SCHEMA_CASE_SENSITIVE;
    PartitionSpec.Builder builder =
        PartitionSpec.builderFor(schema).year("order_date", "p1").year("order_date", "P1");

    StructType expectedType =
        StructType.of(
            NestedField.optional(1000, "p1", Types.DateType.get()),
            NestedField.optional(1001, "P1", Types.DateType.get()));
    PartitionSpec spec = builder.build();

    TestTables.TestTable table =
        TestTables.create(tableDir, "test", schema, spec, V2_FORMAT_VERSION);

    StructType actualType = Partitioning.partitionType(table);
    assertThat(actualType).isEqualTo(expectedType);
  }

  @Test
  public void testYearSourceNameAllowsInexactDuplicateWhenCaseInsensitive() {
    Schema schema = SCHEMA_CASE_INSENSITIVE;
    PartitionSpec.Builder builder =
        PartitionSpec.builderFor(schema)
            .caseSensitive(false)
            .year("order_date", "p1")
            .year("ORDER_DATE", "P1");

    StructType expectedType =
        StructType.of(
            NestedField.optional(1000, "p1", Types.DateType.get()),
            NestedField.optional(1001, "P1", Types.DateType.get()));
    PartitionSpec spec = builder.build();

    TestTables.TestTable table =
        TestTables.create(tableDir, "test", schema, spec, V2_FORMAT_VERSION);

    StructType actualType = Partitioning.partitionType(table);
    assertThat(actualType).isEqualTo(expectedType);
  }

  @Test
  public void testYearTargetNameAllowsInexactDuplicateWhenCaseInsensitive() {
    Schema schema = SCHEMA_CASE_INSENSITIVE;
    PartitionSpec.Builder builder =
        PartitionSpec.builderFor(schema)
            .caseSensitive(false)
            .year("order_date", "p1")
            .year("ship_date", "P1");

    StructType expectedType =
        StructType.of(
            NestedField.optional(1000, "p1", Types.DateType.get()),
            NestedField.optional(1001, "P1", Types.DateType.get()));
    PartitionSpec spec = builder.build();

    TestTables.TestTable table =
        TestTables.create(tableDir, "test", schema, spec, V2_FORMAT_VERSION);

    StructType actualType = Partitioning.partitionType(table);
    assertThat(actualType).isEqualTo(expectedType);
  }

  @Test
  public void testYearTargetNameDoesNotAllowExactDuplicateWhenCaseInsensitive() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                PartitionSpec.builderFor(SCHEMA_CASE_INSENSITIVE)
                    .caseSensitive(false)
                    .year("order_date", "p1")
                    .year("ship_date", "p1")
                    .build())
        .withMessage("Cannot use partition name more than once: p1");
  }

  @Test
  public void testYearTargetNameDoesNotAllowExactDuplicateWhenCaseSensitive() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                PartitionSpec.builderFor(SCHEMA_CASE_SENSITIVE)
                    .year("order_date", "p1")
                    .year("ORDER_DATE", "p1")
                    .build())
        .withMessage("Cannot use partition name more than once: p1");
  }

  @Test
  public void testMonthSourceNameAllowsExactDuplicateWhenCaseSensitive() {
    Schema schema = SCHEMA_CASE_SENSITIVE;
    PartitionSpec.Builder builder =
        PartitionSpec.builderFor(schema).month("order_date", "p1").month("order_date", "P1");

    StructType expectedType =
        StructType.of(
            NestedField.optional(1000, "p1", Types.DateType.get()),
            NestedField.optional(1001, "P1", Types.DateType.get()));
    PartitionSpec spec = builder.build();

    TestTables.TestTable table =
        TestTables.create(tableDir, "test", schema, spec, V2_FORMAT_VERSION);

    StructType actualType = Partitioning.partitionType(table);
    assertThat(actualType).isEqualTo(expectedType);
  }

  @Test
  public void testMonthSourceNameAllowsInexactDuplicateWhenCaseInsensitive() {
    Schema schema = SCHEMA_CASE_INSENSITIVE;
    PartitionSpec.Builder builder =
        PartitionSpec.builderFor(schema)
            .caseSensitive(false)
            .month("order_date", "p1")
            .month("ORDER_DATE", "P1");

    StructType expectedType =
        StructType.of(
            NestedField.optional(1000, "p1", Types.IntegerType.get()),
            NestedField.optional(1001, "P1", Types.IntegerType.get()));
    PartitionSpec spec = builder.build();

    TestTables.TestTable table =
        TestTables.create(tableDir, "test", schema, spec, V2_FORMAT_VERSION);

    StructType actualType = Partitioning.partitionType(table);
    assertThat(actualType).isEqualTo(expectedType);
  }

  @Test
  public void testMonthTargetNameAllowsInexactDuplicateWhenCaseInsensitive() {
    Schema schema = SCHEMA_CASE_INSENSITIVE;
    PartitionSpec.Builder builder =
        PartitionSpec.builderFor(schema)
            .caseSensitive(false)
            .month("order_date", "p1")
            .month("ship_date", "P1");

    StructType expectedType =
        StructType.of(
            NestedField.optional(1000, "p1", Types.DateType.get()),
            NestedField.optional(1001, "P1", Types.DateType.get()));
    PartitionSpec spec = builder.build();

    TestTables.TestTable table =
        TestTables.create(tableDir, "test", schema, spec, V2_FORMAT_VERSION);

    StructType actualType = Partitioning.partitionType(table);
    assertThat(actualType).isEqualTo(expectedType);
  }

  @Test
  public void testMonthTargetNameDoesNotAllowExactDuplicateWhenCaseInsensitive() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                PartitionSpec.builderFor(SCHEMA_CASE_INSENSITIVE)
                    .caseSensitive(false)
                    .month("order_date", "p1")
                    .month("ship_date", "p1")
                    .build())
        .withMessage("Cannot use partition name more than once: p1");
  }

  @Test
  public void testMonthTargetNameDoesNotAllowExactDuplicateWhenCaseSensitive() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                PartitionSpec.builderFor(SCHEMA_CASE_SENSITIVE)
                    .month("order_date", "p1")
                    .month("ORDER_DATE", "p1")
                    .build())
        .withMessage("Cannot use partition name more than once: p1");
  }

  @Test
  public void testDaySourceNameAllowsExactDuplicateWhenCaseSensitive() {
    Schema schema = SCHEMA_CASE_SENSITIVE;
    PartitionSpec.Builder builder =
        PartitionSpec.builderFor(schema).day("order_date", "p1").day("order_date", "P1");

    StructType expectedType =
        StructType.of(
            NestedField.optional(1000, "p1", Types.DateType.get()),
            NestedField.optional(1001, "P1", Types.DateType.get()));
    PartitionSpec spec = builder.build();

    TestTables.TestTable table =
        TestTables.create(tableDir, "test", schema, spec, V2_FORMAT_VERSION);

    StructType actualType = Partitioning.partitionType(table);
    assertThat(actualType).isEqualTo(expectedType);
  }

  @Test
  public void testDaySourceNameAllowsInexactDuplicateWhenCaseInsensitive() {
    Schema schema = SCHEMA_CASE_INSENSITIVE;
    PartitionSpec.Builder builder =
        PartitionSpec.builderFor(schema)
            .caseSensitive(false)
            .day("order_date", "p1")
            .day("ORDER_DATE", "P1");

    StructType expectedType =
        StructType.of(
            NestedField.optional(1000, "p1", Types.DateType.get()),
            NestedField.optional(1001, "P1", Types.DateType.get()));
    PartitionSpec spec = builder.build();

    TestTables.TestTable table =
        TestTables.create(tableDir, "test", schema, spec, V2_FORMAT_VERSION);

    StructType actualType = Partitioning.partitionType(table);
    assertThat(actualType).isEqualTo(expectedType);
  }

  @Test
  public void testDayTargetNameAllowsInexactDuplicateWhenCaseInsensitive() {
    Schema schema = SCHEMA_CASE_INSENSITIVE;
    PartitionSpec.Builder builder =
        PartitionSpec.builderFor(schema)
            .caseSensitive(false)
            .day("order_date", "p1")
            .day("ship_date", "P1");

    StructType expectedType =
        StructType.of(
            NestedField.optional(1000, "p1", Types.DateType.get()),
            NestedField.optional(1001, "P1", Types.DateType.get()));
    PartitionSpec spec = builder.build();

    TestTables.TestTable table =
        TestTables.create(tableDir, "test", schema, spec, V2_FORMAT_VERSION);

    StructType actualType = Partitioning.partitionType(table);
    assertThat(actualType).isEqualTo(expectedType);
  }

  @Test
  public void testDayTargetNameDoesNotAllowExactDuplicateWhenCaseInsensitive() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                PartitionSpec.builderFor(SCHEMA_CASE_INSENSITIVE)
                    .caseSensitive(false)
                    .day("order_date", "p1")
                    .day("ship_date", "p1")
                    .build())
        .withMessage("Cannot use partition name more than once: p1");
  }

  @Test
  public void testDayTargetNameDoesNotAllowExactDuplicateWhenCaseSensitive() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                PartitionSpec.builderFor(SCHEMA_CASE_SENSITIVE)
                    .day("order_date", "p1")
                    .day("ORDER_DATE", "p1")
                    .build())
        .withMessage("Cannot use partition name more than once: p1");
  }

  @Test
  public void testHourSourceNameAllowsExactDuplicateWhenCaseSensitive() {
    Schema schema = SCHEMA_CASE_SENSITIVE;
    PartitionSpec.Builder builder =
        PartitionSpec.builderFor(schema).hour("order_time", "p1").hour("order_time", "P1");

    StructType expectedType =
        StructType.of(
            NestedField.optional(1000, "p1", Types.TimestampType.withoutZone()),
            NestedField.optional(1001, "P1", Types.TimestampType.withoutZone()));
    PartitionSpec spec = builder.build();

    TestTables.TestTable table =
        TestTables.create(tableDir, "test", schema, spec, V2_FORMAT_VERSION);

    StructType actualType = Partitioning.partitionType(table);
    assertThat(actualType).isEqualTo(expectedType);
  }

  @Test
  public void testHourSourceNameAllowsInexactDuplicateWhenCaseInsensitive() {
    Schema schema = SCHEMA_CASE_INSENSITIVE;
    PartitionSpec.Builder builder =
        PartitionSpec.builderFor(schema)
            .caseSensitive(false)
            .hour("order_time", "p1")
            .hour("ORDER_TIME", "P1");

    StructType expectedType =
        StructType.of(
            NestedField.optional(1000, "p1", Types.TimestampType.withoutZone()),
            NestedField.optional(1001, "P1", Types.TimestampType.withoutZone()));
    PartitionSpec spec = builder.build();

    TestTables.TestTable table =
        TestTables.create(tableDir, "test", schema, spec, V2_FORMAT_VERSION);

    StructType actualType = Partitioning.partitionType(table);
    assertThat(actualType).isEqualTo(expectedType);
  }

  @Test
  public void testHourTargetNameAllowsInexactDuplicateWhenCaseInsensitive() {
    Schema schema = SCHEMA_CASE_INSENSITIVE;
    PartitionSpec.Builder builder =
        PartitionSpec.builderFor(schema)
            .caseSensitive(false)
            .hour("order_time", "p1")
            .hour("ship_time", "P1");

    StructType expectedType =
        StructType.of(
            NestedField.optional(1000, "p1", Types.TimestampType.withoutZone()),
            NestedField.optional(1001, "P1", Types.TimestampType.withoutZone()));
    PartitionSpec spec = builder.build();

    TestTables.TestTable table =
        TestTables.create(tableDir, "test", schema, spec, V2_FORMAT_VERSION);

    StructType actualType = Partitioning.partitionType(table);
    assertThat(actualType).isEqualTo(expectedType);
  }

  @Test
  public void testHourTargetNameDoesNotAllowExactDuplicateWhenCaseInsensitive() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                PartitionSpec.builderFor(SCHEMA_CASE_INSENSITIVE)
                    .caseSensitive(false)
                    .hour("order_time", "p1")
                    .hour("ship_time", "p1")
                    .build())
        .withMessage("Cannot use partition name more than once: p1");
  }

  @Test
  public void testHourTargetNameDoesNotAllowExactDuplicateWhenCaseSensitive() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                PartitionSpec.builderFor(SCHEMA_CASE_SENSITIVE)
                    .hour("order_time", "p1")
                    .hour("ORDER_TIME", "p1")
                    .build())
        .withMessage("Cannot use partition name more than once: p1");
  }
}
