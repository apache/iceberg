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
import java.util.List;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

class TestColumnFileStruct {

  private static final List<Integer> FIELD_IDS = Lists.newArrayList(1, 2, 3);
  private static final String LOCATION = "s3://bucket/data/column.parquet";

  @Test
  void testFieldAccess() {
    ColumnFile columnFile =
        ColumnFileStruct.builder()
            .fieldIds(FIELD_IDS)
            .location(LOCATION)
            .fileSizeInBytes(1024L)
            .build();

    assertThat(columnFile.fieldIds()).containsExactlyElementsOf(FIELD_IDS);
    assertThat(columnFile.location()).isEqualTo(LOCATION);
    assertThat(columnFile.fileSizeInBytes()).isEqualTo(1024L);
  }

  @Test
  void testCopy() {
    ColumnFile columnFile =
        ColumnFileStruct.builder()
            .fieldIds(FIELD_IDS)
            .location(LOCATION)
            .fileSizeInBytes(2048L)
            .build();

    ColumnFile copy = columnFile.copy();

    assertThat(copy.fieldIds()).containsExactlyElementsOf(FIELD_IDS);
    assertThat(copy.location()).isEqualTo(LOCATION);
    assertThat(copy.fileSizeInBytes()).isEqualTo(2048L);

    // verify deep copy
    assertThat(copy.fieldIds()).isNotSameAs(columnFile.fieldIds());
  }

  @Test
  void testStructLikeSize() {
    ColumnFileStruct columnFile = new ColumnFileStruct(ColumnFile.schema());
    assertThat(columnFile.size()).isEqualTo(3);
  }

  @Test
  void testStructLikeGetSet() {
    ColumnFileStruct columnFile = new ColumnFileStruct(ColumnFile.schema());

    columnFile.set(0, Lists.newArrayList(1, 2, 3, 4));
    columnFile.set(1, LOCATION);
    columnFile.set(2, 128L);

    assertThat(columnFile.get(0, List.class)).containsExactly(1, 2, 3, 4);
    assertThat(columnFile.get(1, String.class)).isEqualTo(LOCATION);
    assertThat(columnFile.get(2, Long.class)).isEqualTo(128L);
  }

  @Test
  void testProjectedStructLike() {
    Types.StructType projection =
        Types.StructType.of(ColumnFile.LOCATION, ColumnFile.FILE_SIZE_IN_BYTES);

    ColumnFileStruct columnFile = new ColumnFileStruct(projection);
    assertThat(columnFile.size()).isEqualTo(2);

    // projected position 0 maps to internal position 1 (location)
    // projected position 1 maps to internal position 2 (file_size_in_bytes)
    columnFile.set(0, LOCATION);
    columnFile.set(1, 1024L);

    assertThat(columnFile.location()).isEqualTo(LOCATION);
    assertThat(columnFile.fileSizeInBytes()).isEqualTo(1024L);
    assertThat(columnFile.get(0, String.class)).isEqualTo(LOCATION);
    assertThat(columnFile.get(1, Long.class)).isEqualTo(1024L);
  }

  @Test
  void testInvalidBuilderValues() {
    assertThatThrownBy(
            () ->
                ColumnFileStruct.builder()
                    .fieldIds(null)
                    .location(LOCATION)
                    .fileSizeInBytes(1024L)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid field IDs: null");

    assertThatThrownBy(
            () ->
                ColumnFileStruct.builder()
                    .fieldIds(Lists.newArrayList())
                    .location(LOCATION)
                    .fileSizeInBytes(1024L)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid field IDs: empty");

    assertThatThrownBy(
            () ->
                ColumnFileStruct.builder()
                    .fieldIds(FIELD_IDS)
                    .location(null)
                    .fileSizeInBytes(1024L)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid location: null");

    assertThatThrownBy(
            () ->
                ColumnFileStruct.builder()
                    .fieldIds(FIELD_IDS)
                    .location("")
                    .fileSizeInBytes(1024L)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid location: empty");

    assertThatThrownBy(
            () ->
                ColumnFileStruct.builder()
                    .fieldIds(FIELD_IDS)
                    .location(LOCATION)
                    .fileSizeInBytes(-1)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid file size in bytes: -1 (must be >= 0)");
  }

  @Test
  void testMissingBuilderValues() {
    assertThatThrownBy(
            () -> ColumnFileStruct.builder().location(LOCATION).fileSizeInBytes(1024L).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Missing required value: fieldIds");

    assertThatThrownBy(
            () -> ColumnFileStruct.builder().fieldIds(FIELD_IDS).fileSizeInBytes(1024L).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Missing required value: location");

    assertThatThrownBy(
            () -> ColumnFileStruct.builder().fieldIds(FIELD_IDS).location(LOCATION).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Missing required value: fileSizeInBytes");
  }

  @Test
  void testJavaSerializationRoundTrip() throws IOException, ClassNotFoundException {
    ColumnFile columnFile =
        ColumnFileStruct.builder()
            .fieldIds(FIELD_IDS)
            .location(LOCATION)
            .fileSizeInBytes(1024L)
            .build();

    ColumnFile deserialized = TestHelpers.roundTripSerialize(columnFile);

    assertThat(deserialized.fieldIds()).containsExactlyElementsOf(FIELD_IDS);
    assertThat(deserialized.location()).isEqualTo(LOCATION);
    assertThat(deserialized.fileSizeInBytes()).isEqualTo(1024L);
  }

  @Test
  void testKryoSerializationRoundTrip() throws IOException {
    ColumnFile columnFile =
        ColumnFileStruct.builder()
            .fieldIds(FIELD_IDS)
            .location(LOCATION)
            .fileSizeInBytes(1024L)
            .build();

    ColumnFile deserialized = TestHelpers.KryoHelpers.roundTripSerialize(columnFile);

    assertThat(deserialized.fieldIds()).containsExactlyElementsOf(FIELD_IDS);
    assertThat(deserialized.location()).isEqualTo(LOCATION);
    assertThat(deserialized.fileSizeInBytes()).isEqualTo(1024L);
  }
}
