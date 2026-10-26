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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TestFileTypeTableMetadata {
  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()), optional(2, "photo", Types.FileType.of(2)));

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 3})
  void rejectsAFileColumnBeforeFormatVersion4(int formatVersion) {
    assertThatThrownBy(() -> newTableMetadata(formatVersion))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Invalid type for photo: file is not supported until v4");
  }

  @Test
  void acceptsAMetricsModeForAFileNestedField() {
    TableMetadata metadata =
        TableMetadata.newTableMetadata(
            SCHEMA,
            PartitionSpec.unpartitioned(),
            "file:/tmp/table",
            ImmutableMap.of(
                TableProperties.FORMAT_VERSION,
                "4",
                TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "photo.size",
                "full"));

    assertThat(metadata.properties())
        .containsEntry(TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "photo.size", "full");
  }

  @Test
  void replacementReservesDerivedIdsWhenAColumnBecomesAFile() {
    TableMetadata metadata =
        TableMetadata.newTableMetadata(
            new Schema(
                required(1, "id", Types.LongType.get()),
                optional(2, "photo", Types.StringType.get()),
                optional(3, "data", Types.StringType.get())),
            PartitionSpec.unpartitioned(),
            "file:/tmp/table",
            ImmutableMap.of(TableProperties.FORMAT_VERSION, "4"));
    Schema updated =
        new Schema(
            required(100, "id", Types.LongType.get()),
            optional(101, "photo", Types.FileType.of(101)),
            optional(108, "data", Types.StringType.get()),
            optional(109, "notes", Types.StringType.get()));
    Schema expected =
        new Schema(
            required(1, "id", Types.LongType.get()),
            optional(4, "photo", Types.FileType.of(4)),
            optional(3, "data", Types.StringType.get()),
            optional(11, "notes", Types.StringType.get()));

    TableMetadata replacement =
        metadata.buildReplacement(
            updated,
            PartitionSpec.unpartitioned(),
            SortOrder.unsorted(),
            metadata.location(),
            ImmutableMap.of());

    assertThat(replacement.schema().asStruct()).isEqualTo(expected.asStruct());
    assertThat(replacement.lastColumnId()).isEqualTo(expected.highestFieldId());
  }

  private static TableMetadata newTableMetadata(int formatVersion) {
    return TableMetadata.newTableMetadata(
        SCHEMA,
        PartitionSpec.unpartitioned(),
        "file:/tmp/table",
        ImmutableMap.of(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion)));
  }
}
