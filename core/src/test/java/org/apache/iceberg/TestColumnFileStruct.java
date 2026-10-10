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
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class TestColumnFileStruct {

  private static final String LOCATION = "s3://bucket/data/column.parquet";
  private static final List<Integer> FIELD_IDS = Lists.newArrayList(1, 2, 3);
  private static final FileFormat FILE_FORMAT = FileFormat.PARQUET;
  private static final long FILE_SIZE_IN_BYTES = 1024L;
  private static final ByteBuffer KEY_METADATA = ByteBuffer.wrap(new byte[] {1, 2, 3});

  @Test
  void fieldAccess() {
    ColumnFile columnFile =
        new ColumnFileStruct(LOCATION, FIELD_IDS, FILE_FORMAT, FILE_SIZE_IN_BYTES, KEY_METADATA);

    assertThat(columnFile.location()).isEqualTo(LOCATION);
    assertThat(columnFile.fieldIds()).containsExactlyElementsOf(FIELD_IDS);
    assertThat(columnFile.fileFormat()).isEqualTo(FILE_FORMAT);
    assertThat(columnFile.fileSizeInBytes()).isEqualTo(FILE_SIZE_IN_BYTES);
    assertThat(columnFile.keyMetadata()).isEqualTo(KEY_METADATA);
  }

  @Test
  void copy() {
    ColumnFile columnFile =
        new ColumnFileStruct(LOCATION, FIELD_IDS, FILE_FORMAT, FILE_SIZE_IN_BYTES, KEY_METADATA);

    ColumnFile copy = columnFile.copy();

    assertThat(copy.location()).isEqualTo(LOCATION);
    assertThat(copy.fieldIds()).containsExactlyElementsOf(FIELD_IDS);
    assertThat(copy.fileFormat()).isEqualTo(FILE_FORMAT);
    assertThat(copy.fileSizeInBytes()).isEqualTo(FILE_SIZE_IN_BYTES);
    assertThat(copy.keyMetadata()).isEqualTo(KEY_METADATA);
  }

  @Test
  void structLikeSize() {
    ColumnFileStruct columnFile = new ColumnFileStruct();
    assertThat(columnFile.size()).isEqualTo(5);
  }

  @Test
  void setFieldsByOrdinals() {
    ColumnFileStruct columnFile = new ColumnFileStruct();

    columnFile.set(0, LOCATION);
    columnFile.set(1, FIELD_IDS);
    columnFile.set(2, FILE_FORMAT.toString());
    columnFile.set(3, FILE_SIZE_IN_BYTES);
    columnFile.set(4, KEY_METADATA);

    assertThat(columnFile.location()).isEqualTo(LOCATION);
    assertThat(columnFile.fieldIds()).containsExactlyElementsOf(FIELD_IDS);
    assertThat(columnFile.fileFormat()).isEqualTo(FILE_FORMAT);
    assertThat(columnFile.fileSizeInBytes()).isEqualTo(FILE_SIZE_IN_BYTES);
    assertThat(columnFile.keyMetadata()).isEqualTo(KEY_METADATA);
  }

  @Test
  void getFieldsByOrdinals() {
    ColumnFileStruct columnFile =
        new ColumnFileStruct(LOCATION, FIELD_IDS, FILE_FORMAT, FILE_SIZE_IN_BYTES, KEY_METADATA);

    assertThat(columnFile.get(0, String.class)).isEqualTo(LOCATION);
    assertThat(columnFile.get(1, List.class)).containsExactlyElementsOf(FIELD_IDS);
    assertThat(columnFile.get(2, String.class)).isEqualTo(FILE_FORMAT.toString());
    assertThat(columnFile.get(3, Long.class)).isEqualTo(FILE_SIZE_IN_BYTES);
    assertThat(columnFile.get(4, ByteBuffer.class)).isEqualTo(KEY_METADATA);
  }

  @Test
  void projectedStructLike() {
    Types.StructType projection =
        Types.StructType.of(ColumnFile.LOCATION, ColumnFile.FILE_SIZE_IN_BYTES);

    ColumnFileStruct columnFile = new ColumnFileStruct(projection);
    assertThat(columnFile.size()).isEqualTo(2);

    // projected position 0 maps to internal position of location
    // projected position 1 maps to internal position of file_size_in_bytes
    columnFile.set(0, LOCATION);
    columnFile.set(1, FILE_SIZE_IN_BYTES);

    assertThat(columnFile.location()).isEqualTo(LOCATION);
    assertThat(columnFile.fileSizeInBytes()).isEqualTo(FILE_SIZE_IN_BYTES);
    assertThat(columnFile.get(0, String.class)).isEqualTo(LOCATION);
    assertThat(columnFile.get(1, Long.class)).isEqualTo(FILE_SIZE_IN_BYTES);
  }

  @ParameterizedTest
  @MethodSource("org.apache.iceberg.TestHelpers#serializers")
  void serializationRoundTrip(TestHelpers.RoundTripSerializer<ColumnFile> roundTripSerializer)
      throws IOException, ClassNotFoundException {
    ColumnFile columnFile =
        new ColumnFileStruct(LOCATION, FIELD_IDS, FILE_FORMAT, FILE_SIZE_IN_BYTES, KEY_METADATA);

    ColumnFile deserialized = roundTripSerializer.apply(columnFile);

    assertThat((StructLike) deserialized)
        .usingComparator(Comparators.forType(ColumnFile.schema()))
        .isEqualTo(columnFile);
  }

  @Test
  void invalidBuilderValues() {
    assertThatThrownBy(() -> ColumnFileStruct.builder().location(null).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid location: null");

    assertThatThrownBy(() -> ColumnFileStruct.builder().location("").build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid location: empty");

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

    assertThatThrownBy(() -> ColumnFileStruct.builder().fileFormat(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid file format: null");

    assertThatThrownBy(() -> ColumnFileStruct.builder().fileSizeInBytes(-1).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid file size in bytes: -1 (must be >= 0)");
  }

  @Test
  void missingBuilderValues() {
    assertThatThrownBy(
            () ->
                ColumnFileStruct.builder()
                    .fieldIds(FIELD_IDS)
                    .fileFormat(FILE_FORMAT)
                    .fileSizeInBytes(FILE_SIZE_IN_BYTES)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Missing required value: location");

    assertThatThrownBy(
            () ->
                ColumnFileStruct.builder()
                    .location(LOCATION)
                    .fileFormat(FILE_FORMAT)
                    .fileSizeInBytes(FILE_SIZE_IN_BYTES)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Missing required value: field IDs");

    assertThatThrownBy(
            () ->
                ColumnFileStruct.builder()
                    .location(LOCATION)
                    .fieldIds(FIELD_IDS)
                    .fileSizeInBytes(FILE_SIZE_IN_BYTES)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Missing required value: file format");

    assertThatThrownBy(
            () ->
                ColumnFileStruct.builder()
                    .location(LOCATION)
                    .fieldIds(FIELD_IDS)
                    .fileFormat(FILE_FORMAT)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Missing required value: file size in bytes");
  }
}
