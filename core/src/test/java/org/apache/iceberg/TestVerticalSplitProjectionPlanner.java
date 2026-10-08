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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

class TestVerticalSplitProjectionPlanner {
  private static final String DATA_FILE_LOCATION = "s3://bucket/data/file.parquet";
  private static final String COLUMN_FILE_LOCATION = "s3://bucket/data/column-file.parquet";
  private static final Map<Integer, PartitionSpec> UNPARTITIONED =
      ImmutableMap.of(PartitionSpec.unpartitioned().specId(), PartitionSpec.unpartitioned());
  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()),
          Types.NestedField.optional(3, "category", Types.StringType.get()));
  private static final int POSITION_ID = MetadataColumns.ROW_POSITION.fieldId();

  @Test
  void readsEveryFieldFromDataFileWithoutColumnFiles() {
    Map<String, Schema> plan = VerticalSplitProjectionPlanner.plan(dataFile(null), SCHEMA);

    assertThat(plan).hasSize(1);
    assertThat(plan.get(DATA_FILE_LOCATION)).isSameAs(SCHEMA);
  }

  @Test
  void readsColumnFileFieldsFromColumnFile() {
    DataFile file = dataFile(List.of(columnFile(COLUMN_FILE_LOCATION, List.of(2, 3))));

    Map<String, Schema> plan = VerticalSplitProjectionPlanner.plan(file, SCHEMA);

    assertThat(plan.keySet()).containsExactly(DATA_FILE_LOCATION, COLUMN_FILE_LOCATION);
    assertThat(fieldIds(plan.get(DATA_FILE_LOCATION))).containsExactly(1);
    assertThat(fieldIds(plan.get(COLUMN_FILE_LOCATION))).containsExactly(2, 3);
  }

  @Test
  void readsPositionsFromDataFileProvidingNoProjectedField() {
    DataFile file = dataFile(List.of(columnFile(COLUMN_FILE_LOCATION, List.of(1, 2, 3))));

    Map<String, Schema> plan = VerticalSplitProjectionPlanner.plan(file, SCHEMA);

    assertThat(plan.keySet()).containsExactly(DATA_FILE_LOCATION, COLUMN_FILE_LOCATION);
    assertThat(fieldIds(plan.get(DATA_FILE_LOCATION))).containsExactly(POSITION_ID);
    assertThat(fieldIds(plan.get(COLUMN_FILE_LOCATION))).containsExactly(1, 2, 3);
  }

  @Test
  void skipsColumnFilesOutsideProjection() {
    DataFile file = dataFile(List.of(columnFile(COLUMN_FILE_LOCATION, List.of(3))));
    Schema projection = TypeUtil.select(SCHEMA, Set.of(1, 2));

    Map<String, Schema> plan = VerticalSplitProjectionPlanner.plan(file, projection);

    assertThat(plan.keySet()).containsExactly(DATA_FILE_LOCATION);
    assertThat(plan.get(DATA_FILE_LOCATION)).isSameAs(projection);
  }

  @Test
  void readsProjectedPositionsFromDataFile() {
    DataFile file = dataFile(List.of(columnFile(COLUMN_FILE_LOCATION, List.of(2, 3))));
    Schema projection =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "data", Types.StringType.get()),
            MetadataColumns.ROW_POSITION);

    Map<String, Schema> plan = VerticalSplitProjectionPlanner.plan(file, projection);

    assertThat(fieldIds(plan.get(DATA_FILE_LOCATION))).containsExactly(1, POSITION_ID);
    assertThat(fieldIds(plan.get(COLUMN_FILE_LOCATION))).containsExactly(2);
  }

  @Test
  void rejectsInvalidArguments() {
    assertThatThrownBy(() -> VerticalSplitProjectionPlanner.plan(null, SCHEMA))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid data file: null");

    assertThatThrownBy(() -> VerticalSplitProjectionPlanner.plan(dataFile(null), null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid projection: null");
  }

  private static List<Integer> fieldIds(Schema schema) {
    return schema.columns().stream().map(Types.NestedField::fieldId).toList();
  }

  private static ColumnFile columnFile(String location, List<Integer> fieldIds) {
    return ColumnFiles.builder()
        .withFormatVersion(4)
        .withFieldIds(fieldIds)
        .withLocation(location)
        .withFileFormat(FileFormat.PARQUET)
        .withFileSizeInBytes(128L)
        .build();
  }

  private static DataFile dataFile(List<ColumnFile> columnFiles) {
    TrackedFile file =
        new TrackedFileStruct(
            new TrackingStruct(EntryStatus.ADDED, 42L, 10L, 11L, null, 1000L, null, null, null),
            FileContent.DATA,
            4,
            DATA_FILE_LOCATION,
            FileFormat.PARQUET,
            100L,
            1024L,
            PartitionSpec.unpartitioned().specId(),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            columnFiles);

    return TrackedFileAdapters.asDataFile(file, UNPARTITIONED);
  }
}
