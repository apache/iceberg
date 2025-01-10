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
import static org.junit.Assume.assumeTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Path;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.FieldSource;

public class TestRowLineageMetadata {

  private static final String TEST_LOCATION = "s3://bucket/test/location";

  private static final Schema TEST_SCHEMA =
      new Schema(
          7,
          Types.NestedField.required(1, "x", Types.LongType.get()),
          Types.NestedField.required(2, "y", Types.LongType.get(), "comment"),
          Types.NestedField.required(3, "z", Types.LongType.get()));

  private static final long SEQ_NO = 34;
  private static final int LAST_ASSIGNED_COLUMN_ID = 3;

  private static final PartitionSpec SPEC_5 =
      PartitionSpec.builderFor(TEST_SCHEMA).withSpecId(5).build();
  private static final SortOrder SORT_ORDER_3 =
      SortOrder.builderFor(TEST_SCHEMA)
          .withOrderId(3)
          .asc("y", NullOrder.NULLS_FIRST)
          .desc(Expressions.bucket("z", 4), NullOrder.NULLS_LAST)
          .build();

  @TempDir private Path temp;

  public TableOperations ops = new LocalTableOperations(temp);

  private TableMetadata.Builder builderFor(int formatVersion) {
    return TableMetadata.buildFromEmpty(TableMetadata.MIN_FORMAT_VERSION_ROW_LINEAGE)
        .enableRowLineage();
  }

  private TableMetadata baseMetadata(int formatVersion) {
    return builderFor(formatVersion)
        .addSchema(TEST_SCHEMA)
        .setLocation(TEST_LOCATION)
        .addPartitionSpec(PartitionSpec.unpartitioned())
        .addSortOrder(SortOrder.unsorted())
        .build();
  }

  @ParameterizedTest
  @FieldSource("org.apache.iceberg.TestHelpers#ALL_VERSIONS")
  public void testRowLineageSupported(int formatVersion) {
    if (formatVersion == TableMetadata.MIN_FORMAT_VERSION_ROW_LINEAGE) {
      assertThat(builderFor(formatVersion)).isNotNull();
    } else {
      IllegalArgumentException notSupported =
          assertThrows(
              IllegalArgumentException.class,
              () -> TableMetadata.buildFromEmpty(formatVersion).enableRowLineage());
      assertThat(notSupported.getMessage()).contains("Cannot use row lineage");
    }
  }

  @ParameterizedTest
  @FieldSource("org.apache.iceberg.TestHelpers#ALL_VERSIONS")
  public void testLastRowIdMustIncrease(int formatVersion) {
    assumeTrue(formatVersion >= TableMetadata.MIN_FORMAT_VERSION_ROW_LINEAGE);
    assertThat(builderFor(formatVersion).incrementLastRowId(5)).isNotNull();
    IllegalArgumentException noDecrease =
        assertThrows(
            IllegalArgumentException.class, () -> builderFor(formatVersion).incrementLastRowId(-5));
    assertThat(noDecrease.getMessage()).contains("Cannot decrease last-row-id");
  }

  @ParameterizedTest
  @FieldSource("org.apache.iceberg.TestHelpers#ALL_VERSIONS")
  public void testSnapshotAddition(int formatVersion) {
    assumeTrue(formatVersion >= TableMetadata.MIN_FORMAT_VERSION_ROW_LINEAGE);

    Long newRows = 30L;

    TableMetadata base = baseMetadata(formatVersion);

    Snapshot addRows =
        new BaseSnapshot(
            0, 1, null, 0, DataOperations.APPEND, null, 1, "foo", base.lastRowId(), newRows);

    TableMetadata firstAddition = TableMetadata.buildFrom(base).addSnapshot(addRows).build();

    assertThat(firstAddition.lastRowId()).isEqualTo(newRows);

    Snapshot addMoreRows =
        new BaseSnapshot(
            1, 2, 1L, 0, DataOperations.APPEND, null, 1, "foo", firstAddition.lastRowId(), newRows);

    TableMetadata secondAddition =
        TableMetadata.buildFrom(firstAddition).addSnapshot(addMoreRows).build();

    assertThat(secondAddition.lastRowId()).isEqualTo(newRows * 2);
  }

  @ParameterizedTest
  @FieldSource("org.apache.iceberg.TestHelpers#ALL_VERSIONS")
  public void testInvalidSnapshotAddition(int formatVersion) {
    assumeTrue(formatVersion >= TableMetadata.MIN_FORMAT_VERSION_ROW_LINEAGE);

    Long newRows = 30L;

    TableMetadata base = baseMetadata(formatVersion);

    Snapshot invalidLastRow =
        new BaseSnapshot(
            0, 1, null, 0, DataOperations.APPEND, null, 1, "foo", base.lastRowId() - 3, newRows);

    ValidationException invalidLastRowId =
        assertThrows(
            ValidationException.class,
            () -> TableMetadata.buildFrom(base).addSnapshot(invalidLastRow));
    assertThat(invalidLastRowId.getMessage()).contains("Cannot add a snapshot whose first-row-id");

    Snapshot invalidNewRows =
        new BaseSnapshot(
            0, 1, null, 0, DataOperations.APPEND, null, 1, "foo", base.lastRowId(), null);

    ValidationException nullNewRows =
        assertThrows(
            ValidationException.class,
            () -> TableMetadata.buildFrom(base).addSnapshot(invalidNewRows));
    assertThat(nullNewRows.getMessage())
        .contains(
            "Cannot add a snapshot with a null `addedRows` field when row lineage is enabled");
  }
}
