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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestUpdateSchemaConstraintChecks {

  private static final Schema SCHEMA =
      new Schema(
          optional(1, "no_nulls", Types.IntegerType.get()),
          optional(2, "some_nulls", Types.LongType.get()),
          optional(3, "no_stats", Types.StringType.get()),
          optional(
              4,
              "struct",
              Types.StructType.of(
                  optional(5, "list", Types.ListType.ofOptional(6, Types.StringType.get())))));

  private static final String EXCEPTION_MSG_FORMAT =
      "Cannot set a column to required: %s, as it cannot be determined from "
          + "existing metadata metrics that it must not contain null values";

  @Rule public TemporaryFolder temp = new TemporaryFolder();
  private TestTables.TestTable table;

  @Before
  public void before() throws IOException {
    table = TestTables.create(temp.newFolder(), "test", SCHEMA, PartitionSpec.unpartitioned(), 2);
  }

  @After
  public void after() {
    TestTables.clearTables();
  }

  @Test
  public void testFreshTableRequireColumn() {
    makeRequiredAndValidate("no_nulls", "some_nulls", "no_stats", "struct", "struct.list");
  }

  @Test
  public void testRequireColumnOnlyDataFiles() {
    addDataFileToTable(10L, ImmutableMap.of(1, 0L, 2, 3L));

    makeRequiredAndValidate("no_nulls");

    makeRequiredAndCapture("some_nulls");
    makeRequiredAndCapture("no_stats");
    makeRequiredAndCapture("struct");
  }

  @Test
  public void testRequireColumnWithEqualityDeletes() {
    addDataFileToTable(10L, ImmutableMap.of(1, 0L, 2, 0L, 3, 0L));
    addEqualityDeleteToTable(10L, ImmutableMap.of(1, 0L, 2, 3L), 1, 2, 3);

    makeRequiredAndValidate("no_nulls");

    makeRequiredAndCapture("some_nulls");
    makeRequiredAndCapture("no_stats");
  }

  @Test
  public void testRequireColumnWithDeletedDataFileHasNulls() {
    addDataFileToTable(10L, ImmutableMap.of(1, 0L, 2, 3L));

    table.newDelete().deleteFromRowFilter(Expressions.alwaysTrue()).commit();
    assertThat(table.newScan().planFiles()).isEmpty();

    addDataFileToTable(10L, ImmutableMap.of(1, 0L, 2, 0L));

    makeRequiredAndValidate("no_nulls");

    makeRequiredAndCapture("some_nulls");
  }

  @Test
  public void testRequireColumnDeletedDeleteFileHasNulls() {
    addEqualityDeleteToTable(10L, ImmutableMap.of(1, 0L, 2, 3L), 1, 2);

    table.newDelete().deleteFromRowFilter(Expressions.alwaysTrue()).commit();
    Iterable<DeleteFile> deleteFiles = table.currentSnapshot().removedDeleteFiles(table.ops().io());
    assertThat(deleteFiles).hasOnlyElementsOfType(DeleteFile.class);

    addEqualityDeleteToTable(10L, ImmutableMap.of(1, 0L, 2, 0L), 1, 2);

    makeRequiredAndValidate("no_nulls");

    makeRequiredAndCapture("some_nulls");
  }

  @Test
  public void testFreshTableAddRequiredColumns() {
    table
        .updateSchema()
        .addRequiredColumn(
            "point",
            Types.StructType.of(
                required(9, "x", Types.LongType.get()), required(10, "y", Types.LongType.get())))
        .commit();

    assertColumnsRequired("point", "point.x", "point.y");
  }

  @Test
  public void testOnlyValidateLiveData() {
    addDataFileToTable(10L, ImmutableMap.of(1, 0L, 2, 3L));

    table.newDelete().deleteFromRowFilter(Expressions.alwaysTrue()).commit();
    assertThat(table.newScan().planFiles()).isEmpty();

    table
        .updateSchema()
        .validateLiveDataOnly()
        .addRequiredColumn("added", Types.DoubleType.get())
        .commit();
    assertColumnsRequired("added");

    addDataFileToTable(10L, ImmutableMap.of(1, 0L, 2, 0L));

    makeRequiredAndValidate(true, false, "some_nulls");
  }

  @Test
  public void testAllowIncompatibleChanges() {
    addDataFileToTable(10L, ImmutableMap.of(1, 0L, 2, 3L));

    makeRequiredAndCapture("some_nulls");
    makeRequiredAndValidate(false, true, "some_nulls");

    Assertions.assertThatThrownBy(
            () -> table.updateSchema().addRequiredColumn("added", Types.DoubleType.get()).commit())
        .isInstanceOf(ValidationException.class)
        .hasMessage(String.format(EXCEPTION_MSG_FORMAT, "added"));

    table
        .updateSchema()
        .allowIncompatibleChanges()
        .addRequiredColumn("added", Types.DoubleType.get())
        .commit();
    assertColumnsRequired("added");
  }

  private void makeRequiredAndCapture(String column) {
    Assertions.assertThatThrownBy(() -> applyUpdate(false, false, column))
        .isInstanceOf(ValidationException.class)
        .hasMessage(String.format(EXCEPTION_MSG_FORMAT, column));
  }

  private void makeRequiredAndValidate(String... columns) {
    makeRequiredAndValidate(false, false, columns);
  }

  private void makeRequiredAndValidate(
      boolean onlyValidateLiveData, boolean allowIncompatibleChanges, String... columns) {

    applyUpdate(onlyValidateLiveData, allowIncompatibleChanges, columns);

    assertColumnsRequired(columns);
  }

  private void applyUpdate(
      boolean onlyValidateLiveData, boolean allowIncompatibleChanges, String... columns) {
    UpdateSchema updateSchema = table.updateSchema();
    if (onlyValidateLiveData) {
      updateSchema.validateLiveDataOnly();
    }

    if (allowIncompatibleChanges) {
      updateSchema.allowIncompatibleChanges();
    }

    for (String column : columns) {
      updateSchema.requireColumn(column);
    }

    updateSchema.commit();
  }

  private void assertColumnsRequired(String... columns) {
    for (String column : columns) {
      assertThat(table.schema().findField(column).isRequired())
          .overridingErrorMessage(
              String.format("Expect column to be required but is optional: %s", column))
          .isTrue();
    }
  }

  private void addDataFileToTable(Long rowCount, Map<Integer, Long> nullCounts) {
    Metrics metrics = new Metrics(10L, null, null, nullCounts, null);
    DataFile dataFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath(UUID.randomUUID() + ".parquet")
            .withFileSizeInBytes(10)
            .withMetrics(metrics)
            .build();

    table.newAppend().appendFile(dataFile).commit();
  }

  private void addEqualityDeleteToTable(
      Long rowCount, Map<Integer, Long> nullCounts, int... eqFieldIds) {
    Metrics metrics = new Metrics(rowCount, null, null, nullCounts, null);
    DeleteFile eqDelete =
        FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
            .ofEqualityDeletes(eqFieldIds)
            .withPath(UUID.randomUUID() + ".parquet")
            .withFileSizeInBytes(100)
            .withMetrics(metrics)
            .build();

    table.newRowDelta().addDeletes(eqDelete).commit();
  }
}
