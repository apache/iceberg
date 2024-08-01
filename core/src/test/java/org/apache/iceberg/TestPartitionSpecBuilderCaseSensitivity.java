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
  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get()),
          required(2, "data", Types.StringType.get()),
          required(4, "date_time", Types.DateType.get()));

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
            required(4, "date_time", Types.DateType.get()));
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
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("data", "p1").build();
    TestTables.TestTable table =
        TestTables.create(tableDir, "test", SCHEMA, spec, V2_FORMAT_VERSION);

    StructType expectedType =
        StructType.of(NestedField.optional(1000, "p1", Types.StringType.get()));
    StructType actualType = Partitioning.partitionType(table);
    assertThat(actualType).isEqualTo(expectedType);
  }

  @Test
  public void testBuilderDoesNotAllowDuplicateIdentitySourceNameWhenCaseSensitive() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                PartitionSpec.builderFor(SCHEMA)
                    .identity("data", "p1")
                    .identity("data", "P1")
                    .build())
        .withMessageStartingWith("Cannot add redundant partition:");
  }

  @Test
  public void testBuilderDoesNotAllowDuplicateIdentityTargetNameWhenCaseInsensitive() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                PartitionSpec.builderFor(SCHEMA)
                    .caseSensitive(false)
                    .identity("data", "p1")
                    .identity("data", "P1")
                    .build())
        .withMessage("Cannot use partition name more than once: P1");
  }

  @Test
  public void testBuilderAllowsDuplicateBucketTargetNameWhenCaseSensitive() {
    PartitionSpec spec =
        PartitionSpec.builderFor(SCHEMA).bucket("data", 10, "p1").bucket("data", 10, "P1").build();

    TestTables.TestTable table =
        TestTables.create(tableDir, "test", SCHEMA, spec, V2_FORMAT_VERSION);

    StructType expectedType =
        StructType.of(
            NestedField.optional(1000, "p1", Types.IntegerType.get()),
            NestedField.optional(1001, "P1", Types.IntegerType.get()));
    StructType actualType = Partitioning.partitionType(table);
    assertThat(actualType).isEqualTo(expectedType);
  }

  @Test
  public void testBuilderDoesNotAllowDuplicateBucketTargetNameWhenCaseInsensitive() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                PartitionSpec.builderFor(SCHEMA)
                    .caseSensitive(false)
                    .bucket("data", 10, "p1")
                    .bucket("data", 10, "P1")
                    .build())
        .withMessage("Cannot use partition name more than once: P1");
  }

  @Test
  public void testBuilderAllowsDuplicateTruncateTargetNameWhenCaseSensitive() {
    PartitionSpec spec =
        PartitionSpec.builderFor(SCHEMA)
            .truncate("data", 10, "p1")
            .truncate("data", 10, "P1")
            .build();

    TestTables.TestTable table =
        TestTables.create(tableDir, "test", SCHEMA, spec, V2_FORMAT_VERSION);

    StructType expectedType =
        StructType.of(
            NestedField.optional(1000, "p1", Types.StringType.get()),
            NestedField.optional(1001, "P1", Types.StringType.get()));
    StructType actualType = Partitioning.partitionType(table);
    assertThat(actualType).isEqualTo(expectedType);
  }

  @Test
  public void testBuilderDoesNotAllowDuplicateTruncateTargetNameWhenCaseInsensitive() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                PartitionSpec.builderFor(SCHEMA)
                    .caseSensitive(false)
                    .truncate("data", 10, "p1")
                    .truncate("data", 10, "P1")
                    .build())
        .withMessage("Cannot use partition name more than once: P1");
  }

  @Test
  public void testBuilderAllowsDuplicateAlwaysNullTargetNameWhenCaseSensitive() {
    PartitionSpec spec =
        PartitionSpec.builderFor(SCHEMA).alwaysNull("data", "p1").alwaysNull("data", "P1").build();

    TestTables.TestTable table =
        TestTables.create(tableDir, "test", SCHEMA, spec, V2_FORMAT_VERSION);

    StructType expectedType =
        StructType.of(
            NestedField.optional(1000, "p1", Types.StringType.get()),
            NestedField.optional(1001, "P1", Types.StringType.get()));
    StructType actualType = Partitioning.partitionType(table);
    assertThat(actualType).isEqualTo(expectedType);
  }

  @Test
  public void testBuilderDoesNotAllowDuplicateAlwaysNullTargetNameWhenCaseInsensitive() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                PartitionSpec.builderFor(SCHEMA)
                    .caseSensitive(false)
                    .alwaysNull("data", "p1")
                    .alwaysNull("data", "P1")
                    .build())
        .withMessage("Cannot use partition name more than once: P1");
  }

  @Test
  public void testBuilderDoesNotAllowDuplicateYearSourceNameWhenCaseSensitive() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                PartitionSpec.builderFor(SCHEMA)
                    .year("date_time", "p1")
                    .year("date_time", "P1")
                    .build())
        .withMessageStartingWith("Cannot add redundant partition:");
  }

  @Test
  public void testBuilderDoesNotAllowDuplicateYearTargetNameWhenCaseInsensitive() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                PartitionSpec.builderFor(SCHEMA)
                    .caseSensitive(false)
                    .year("date_time", "p1")
                    .year("date_time", "P1")
                    .build())
        .withMessage("Cannot use partition name more than once: P1");
  }

  @Test
  public void testBuilderDoesNotAllowDuplicateMonthSourceNameWhenCaseSensitive() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                PartitionSpec.builderFor(SCHEMA)
                    .month("date_time", "p1")
                    .month("date_time", "P1")
                    .build())
        .withMessageStartingWith("Cannot add redundant partition:");
  }

  @Test
  public void testBuilderDoesNotAllowDuplicateMonthTargetNameWhenCaseInsensitive() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                PartitionSpec.builderFor(SCHEMA)
                    .caseSensitive(false)
                    .month("date_time", "p1")
                    .month("date_time", "P1")
                    .build())
        .withMessage("Cannot use partition name more than once: P1");
  }

  @Test
  public void testBuilderDoesNotAllowDuplicateDaySourceNameWhenCaseSensitive() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                PartitionSpec.builderFor(SCHEMA)
                    .day("date_time", "p1")
                    .day("date_time", "P1")
                    .build())
        .withMessageStartingWith("Cannot add redundant partition:");
  }

  @Test
  public void testBuilderDoesNotAllowDuplicateDayTargetNameWhenCaseInsensitive() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                PartitionSpec.builderFor(SCHEMA)
                    .caseSensitive(false)
                    .day("date_time", "p1")
                    .day("date_time", "P1")
                    .build())
        .withMessage("Cannot use partition name more than once: P1");
  }

  @Test
  public void testBuilderDoesNotAllowDuplicateHourSourceNameWhenCaseSensitive() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                PartitionSpec.builderFor(SCHEMA)
                    .hour("date_time", "p1")
                    .hour("date_time", "P1")
                    .build())
        .withMessageStartingWith("Cannot add redundant partition:");
  }

  @Test
  public void testBuilderDoesNotAllowDuplicateHourTargetNameWhenCaseInsensitive() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                PartitionSpec.builderFor(SCHEMA)
                    .caseSensitive(false)
                    .hour("date_time", "p1")
                    .hour("date_time", "P1")
                    .build())
        .withMessage("Cannot use partition name more than once: P1");
  }
}
