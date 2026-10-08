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

import java.nio.ByteBuffer;
import java.util.List;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

class TestColumnFiles {

  private static final int FORMAT_VERSION = 4;
  private static final List<Integer> FIELD_IDS = Lists.newArrayList(1, 2, 3);
  private static final String LOCATION = "s3://bucket/data/column.parquet";
  private static final FileFormat FILE_FORMAT = FileFormat.PARQUET;
  private static final long FILE_SIZE_IN_BYTES = 1024L;
  private static final ByteBuffer KEY_METADATA = ByteBuffer.wrap(new byte[] {1, 2, 3});
  private static final List<Long> SPLIT_OFFSETS = Lists.newArrayList(0L, 512L);

  @Test
  void buildWithAllValues() {
    ColumnFile columnFile =
        ColumnFiles.builder()
            .withFormatVersion(FORMAT_VERSION)
            .withFieldIds(FIELD_IDS)
            .withLocation(LOCATION)
            .withFileFormat(FILE_FORMAT)
            .withFileSizeInBytes(FILE_SIZE_IN_BYTES)
            .withKeyMetadata(KEY_METADATA)
            .withSplitOffsets(SPLIT_OFFSETS)
            .build();

    assertThat(columnFile.formatVersion()).isEqualTo(FORMAT_VERSION);
    assertThat(columnFile.fieldIds()).isEqualTo(FIELD_IDS);
    assertThat(columnFile.location()).isEqualTo(LOCATION);
    assertThat(columnFile.fileFormat()).isEqualTo(FILE_FORMAT);
    assertThat(columnFile.fileSizeInBytes()).isEqualTo(FILE_SIZE_IN_BYTES);
    assertThat(columnFile.keyMetadata()).isEqualTo(KEY_METADATA);
    assertThat(columnFile.splitOffsets()).isEqualTo(SPLIT_OFFSETS);
  }

  @Test
  void buildWithoutOptionalValues() {
    ColumnFile columnFile =
        ColumnFiles.builder()
            .withFormatVersion(FORMAT_VERSION)
            .withFieldIds(FIELD_IDS)
            .withLocation(LOCATION)
            .withFileFormat(FILE_FORMAT)
            .withFileSizeInBytes(FILE_SIZE_IN_BYTES)
            .build();

    assertThat(columnFile.keyMetadata()).isNull();
    assertThat(columnFile.splitOffsets()).isNull();
  }

  @Test
  void invalidBuilderValues() {
    assertThatThrownBy(() -> ColumnFiles.builder().withFieldIds(null).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid field IDs: null");

    assertThatThrownBy(() -> ColumnFiles.builder().withFieldIds(Lists.newArrayList()).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid field IDs: empty");

    assertThatThrownBy(
            () -> ColumnFiles.builder().withFieldIds(Lists.newArrayList(1, 2, 1)).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid field IDs: duplicated IDs found in: [1, 2, 1]");

    assertThatThrownBy(() -> ColumnFiles.builder().withLocation(null).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid location: null");

    assertThatThrownBy(() -> ColumnFiles.builder().withLocation("").build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid location: empty");

    assertThatThrownBy(() -> ColumnFiles.builder().withFileFormat(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid file format: null");

    assertThatThrownBy(() -> ColumnFiles.builder().withFileSizeInBytes(-1).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid file size in bytes: -1 (must be >= 0)");

    assertThatThrownBy(() -> ColumnFiles.builder().withFormatVersion(-1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid format version: -1 (must be >= 0)");

    assertThatThrownBy(() -> ColumnFiles.builder().withKeyMetadata(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid key metadata: null");

    assertThatThrownBy(() -> ColumnFiles.builder().withSplitOffsets(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid split offsets: null");
  }

  @Test
  void missingBuilderValues() {
    assertThatThrownBy(
            () ->
                ColumnFiles.builder()
                    .withFieldIds(FIELD_IDS)
                    .withLocation(LOCATION)
                    .withFileFormat(FILE_FORMAT)
                    .withFileSizeInBytes(FILE_SIZE_IN_BYTES)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Missing required value: format version");

    assertThatThrownBy(
            () ->
                ColumnFiles.builder()
                    .withFormatVersion(FORMAT_VERSION)
                    .withLocation(LOCATION)
                    .withFileFormat(FILE_FORMAT)
                    .withFileSizeInBytes(FILE_SIZE_IN_BYTES)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Missing required value: field IDs");

    assertThatThrownBy(
            () ->
                ColumnFiles.builder()
                    .withFormatVersion(FORMAT_VERSION)
                    .withFieldIds(FIELD_IDS)
                    .withFileFormat(FILE_FORMAT)
                    .withFileSizeInBytes(FILE_SIZE_IN_BYTES)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Missing required value: location");

    assertThatThrownBy(
            () ->
                ColumnFiles.builder()
                    .withFormatVersion(FORMAT_VERSION)
                    .withFieldIds(FIELD_IDS)
                    .withLocation(LOCATION)
                    .withFileSizeInBytes(FILE_SIZE_IN_BYTES)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Missing required value: file format");

    assertThatThrownBy(
            () ->
                ColumnFiles.builder()
                    .withFormatVersion(FORMAT_VERSION)
                    .withFieldIds(FIELD_IDS)
                    .withLocation(LOCATION)
                    .withFileFormat(FILE_FORMAT)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Missing required value: file size in bytes");
  }
}
