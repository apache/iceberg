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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class TestColumnFileStruct {

  private static final int FORMAT_VERSION = 4;
  private static final List<Integer> FIELD_IDS = Lists.newArrayList(1, 2, 3);
  private static final String LOCATION = "s3://bucket/data/column.parquet";
  private static final FileFormat FILE_FORMAT = FileFormat.PARQUET;
  private static final long FILE_SIZE_IN_BYTES = 1024L;
  private static final ByteBuffer KEY_METADATA = ByteBuffer.wrap(new byte[] {1, 2, 3});
  private static final List<Long> SPLIT_OFFSETS = Lists.newArrayList(0L, 512L);

  @Test
  void testFieldAccess() {
    ColumnFile columnFile =
        new ColumnFileStruct(
            FORMAT_VERSION,
            FIELD_IDS,
            LOCATION,
            FILE_FORMAT,
            FILE_SIZE_IN_BYTES,
            KEY_METADATA,
            SPLIT_OFFSETS);

    assertThat(columnFile.formatVersion()).isEqualTo(FORMAT_VERSION);
    assertThat(columnFile.fieldIds()).containsExactlyElementsOf(FIELD_IDS);
    assertThat(columnFile.location()).isEqualTo(LOCATION);
    assertThat(columnFile.fileFormat()).isEqualTo(FILE_FORMAT);
    assertThat(columnFile.fileSizeInBytes()).isEqualTo(FILE_SIZE_IN_BYTES);
    assertThat(columnFile.keyMetadata()).isEqualTo(KEY_METADATA);
    assertThat(columnFile.splitOffsets()).containsExactlyElementsOf(SPLIT_OFFSETS);
  }

  @Test
  void testCopy() {
    ColumnFile columnFile =
        new ColumnFileStruct(
            FORMAT_VERSION,
            FIELD_IDS,
            LOCATION,
            FILE_FORMAT,
            FILE_SIZE_IN_BYTES,
            KEY_METADATA,
            SPLIT_OFFSETS);

    ColumnFile copy = columnFile.copy();

    assertThat(copy.formatVersion()).isEqualTo(FORMAT_VERSION);
    assertThat(copy.fieldIds()).containsExactlyElementsOf(FIELD_IDS);
    assertThat(copy.location()).isEqualTo(LOCATION);
    assertThat(copy.fileFormat()).isEqualTo(FILE_FORMAT);
    assertThat(copy.fileSizeInBytes()).isEqualTo(FILE_SIZE_IN_BYTES);
    assertThat(copy.keyMetadata()).isEqualTo(KEY_METADATA);
    assertThat(copy.splitOffsets()).containsExactlyElementsOf(SPLIT_OFFSETS);
  }

  @Test
  void testStructLikeSize() {
    ColumnFileStruct columnFile = new ColumnFileStruct();
    assertThat(columnFile.size()).isEqualTo(7);
  }

  @Test
  void testSetFieldsByOrdinals() {
    ColumnFileStruct columnFile = new ColumnFileStruct();

    columnFile.set(0, FORMAT_VERSION);
    columnFile.set(1, FIELD_IDS);
    columnFile.set(2, LOCATION);
    columnFile.set(3, FILE_FORMAT.toString());
    columnFile.set(4, FILE_SIZE_IN_BYTES);
    columnFile.set(5, KEY_METADATA);
    columnFile.set(6, SPLIT_OFFSETS);

    assertThat(columnFile.formatVersion()).isEqualTo(FORMAT_VERSION);
    assertThat(columnFile.fieldIds()).containsExactlyElementsOf(FIELD_IDS);
    assertThat(columnFile.location()).isEqualTo(LOCATION);
    assertThat(columnFile.fileFormat()).isEqualTo(FILE_FORMAT);
    assertThat(columnFile.fileSizeInBytes()).isEqualTo(FILE_SIZE_IN_BYTES);
    assertThat(columnFile.keyMetadata()).isEqualTo(KEY_METADATA);
    assertThat(columnFile.splitOffsets()).containsExactlyElementsOf(SPLIT_OFFSETS);
  }

  @Test
  void testGetFieldsByOrdinals() {
    ColumnFileStruct columnFile =
        new ColumnFileStruct(
            FORMAT_VERSION,
            FIELD_IDS,
            LOCATION,
            FILE_FORMAT,
            FILE_SIZE_IN_BYTES,
            KEY_METADATA,
            SPLIT_OFFSETS);

    assertThat(columnFile.get(0, Integer.class)).isEqualTo(FORMAT_VERSION);
    assertThat(columnFile.get(1, List.class)).containsExactlyElementsOf(FIELD_IDS);
    assertThat(columnFile.get(2, String.class)).isEqualTo(LOCATION);
    assertThat(columnFile.get(3, String.class)).isEqualTo(FILE_FORMAT.toString());
    assertThat(columnFile.get(4, Long.class)).isEqualTo(FILE_SIZE_IN_BYTES);
    assertThat(columnFile.get(5, ByteBuffer.class)).isEqualTo(KEY_METADATA);
    assertThat(columnFile.get(6, List.class)).containsExactlyElementsOf(SPLIT_OFFSETS);
  }

  @Test
  void testProjectedStructLike() {
    Types.StructType projection =
        Types.StructType.of(ColumnFile.LOCATION, ColumnFile.FILE_SIZE_IN_BYTES);

    ColumnFileStruct columnFile = new ColumnFileStruct(projection);
    assertThat(columnFile.size()).isEqualTo(2);

    // projected position 0 maps to internal position of location
    // projected position 1 maps to internal position of file_size_in_bytes
    columnFile.set(0, LOCATION);
    columnFile.set(1, 1024L);

    assertThat(columnFile.location()).isEqualTo(LOCATION);
    assertThat(columnFile.fileSizeInBytes()).isEqualTo(1024L);
    assertThat(columnFile.get(0, String.class)).isEqualTo(LOCATION);
    assertThat(columnFile.get(1, Long.class)).isEqualTo(1024L);
  }

  @ParameterizedTest
  @MethodSource("org.apache.iceberg.TestHelpers#serializers")
  void testSerializationRoundTrip(TestHelpers.RoundTripSerializer<ColumnFile> roundTripSerializer)
      throws IOException, ClassNotFoundException {
    ColumnFile columnFile =
        new ColumnFileStruct(
            FORMAT_VERSION,
            FIELD_IDS,
            LOCATION,
            FILE_FORMAT,
            FILE_SIZE_IN_BYTES,
            KEY_METADATA,
            SPLIT_OFFSETS);

    ColumnFile deserialized = roundTripSerializer.apply(columnFile);

    assertThat(deserialized.formatVersion()).isEqualTo(FORMAT_VERSION);
    assertThat(deserialized.fieldIds()).containsExactlyElementsOf(FIELD_IDS);
    assertThat(deserialized.location()).isEqualTo(LOCATION);
    assertThat(deserialized.fileFormat()).isEqualTo(FILE_FORMAT);
    assertThat(deserialized.fileSizeInBytes()).isEqualTo(FILE_SIZE_IN_BYTES);
    assertThat(deserialized.keyMetadata()).isEqualTo(KEY_METADATA);
    assertThat(deserialized.splitOffsets()).containsExactlyElementsOf(SPLIT_OFFSETS);
  }

  @Test
  void testInvalidBuilderValues() {
    assertThatThrownBy(() -> ColumnFileStruct.builder().fieldIds(null).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid field IDs: null");

    assertThatThrownBy(() -> ColumnFileStruct.builder().fieldIds(Lists.newArrayList()).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid field IDs: empty");

    assertThatThrownBy(
            () -> ColumnFileStruct.builder().fieldIds(Lists.newArrayList(1, 2, 1)).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid field IDs: duplicated IDs found in: [1, 2, 1]");

    assertThatThrownBy(() -> ColumnFileStruct.builder().location(null).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid location: null");

    assertThatThrownBy(() -> ColumnFileStruct.builder().location("").build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid location: empty");

    assertThatThrownBy(() -> ColumnFileStruct.builder().fileFormat(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid file format: null");

    assertThatThrownBy(() -> ColumnFileStruct.builder().fileSizeInBytes(-1).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid file size in bytes: -1 (must be >= 0)");

    assertThatThrownBy(() -> ColumnFileStruct.builder().formatVersion(-1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid format version: -1 (must be >= 0)");

    assertThatThrownBy(() -> ColumnFileStruct.builder().keyMetadata(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid key metadata: null");

    assertThatThrownBy(() -> ColumnFileStruct.builder().splitOffsets(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid split offsets: null");
  }

  @Test
  void testMissingBuilderValues() {
    assertThatThrownBy(
            () ->
                ColumnFileStruct.builder()
                    .fieldIds(FIELD_IDS)
                    .location(LOCATION)
                    .fileFormat(FILE_FORMAT)
                    .fileSizeInBytes(FILE_SIZE_IN_BYTES)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Missing required value: formatVersion");

    assertThatThrownBy(
            () ->
                ColumnFileStruct.builder()
                    .formatVersion(FORMAT_VERSION)
                    .location(LOCATION)
                    .fileFormat(FILE_FORMAT)
                    .fileSizeInBytes(FILE_SIZE_IN_BYTES)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Missing required value: fieldIds");

    assertThatThrownBy(
            () ->
                ColumnFileStruct.builder()
                    .formatVersion(FORMAT_VERSION)
                    .fieldIds(FIELD_IDS)
                    .fileFormat(FILE_FORMAT)
                    .fileSizeInBytes(FILE_SIZE_IN_BYTES)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Missing required value: location");

    assertThatThrownBy(
            () ->
                ColumnFileStruct.builder()
                    .formatVersion(FORMAT_VERSION)
                    .fieldIds(FIELD_IDS)
                    .location(LOCATION)
                    .fileSizeInBytes(FILE_SIZE_IN_BYTES)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Missing required value: fileFormat");

    assertThatThrownBy(
            () ->
                ColumnFileStruct.builder()
                    .formatVersion(FORMAT_VERSION)
                    .fieldIds(FIELD_IDS)
                    .location(LOCATION)
                    .fileFormat(FILE_FORMAT)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Missing required value: fileSizeInBytes");
  }
}
