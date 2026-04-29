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
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

class TestManifestInfoStruct {

  @Test
  void testFieldAccess() {
    ManifestInfoStruct info = new ManifestInfoStruct(ManifestInfo.schema());

    info.set(0, 10);
    info.set(1, 20);
    info.set(2, 3);
    info.set(3, 2);
    info.set(4, 1000L);
    info.set(5, 2000L);
    info.set(6, 300L);
    info.set(7, 200L);
    info.set(8, 5L);
    info.set(9, ByteBuffer.wrap(new byte[] {0xF}));
    info.set(10, 1L);

    assertThat(info.addedFilesCount()).isEqualTo(10);
    assertThat(info.existingFilesCount()).isEqualTo(20);
    assertThat(info.deletedFilesCount()).isEqualTo(3);
    assertThat(info.replacedFilesCount()).isEqualTo(2);
    assertThat(info.addedRowsCount()).isEqualTo(1000L);
    assertThat(info.existingRowsCount()).isEqualTo(2000L);
    assertThat(info.deletedRowsCount()).isEqualTo(300L);
    assertThat(info.replacedRowsCount()).isEqualTo(200L);
    assertThat(info.minSequenceNumber()).isEqualTo(5L);
    assertThat(info.dv()).isNotNull();
    assertThat(info.dvCardinality()).isEqualTo(1L);
  }

  @Test
  void testCopy() {
    ManifestInfoStruct info =
        ManifestInfoStruct.builder()
            .addedFilesCount(10)
            .existingFilesCount(20)
            .deletedFilesCount(3)
            .replacedFilesCount(2)
            .addedRowsCount(1000L)
            .existingRowsCount(2000L)
            .deletedRowsCount(300L)
            .replacedRowsCount(200L)
            .minSequenceNumber(5L)
            .dv(new byte[] {0xF})
            .dvCardinality(1L)
            .build();

    ManifestInfoStruct copy = info.copy();

    assertThat(copy.addedFilesCount()).isEqualTo(10);
    assertThat(copy.existingFilesCount()).isEqualTo(20);
    assertThat(copy.deletedFilesCount()).isEqualTo(3);
    assertThat(copy.replacedFilesCount()).isEqualTo(2);
    assertThat(copy.addedRowsCount()).isEqualTo(1000L);
    assertThat(copy.existingRowsCount()).isEqualTo(2000L);
    assertThat(copy.deletedRowsCount()).isEqualTo(300L);
    assertThat(copy.replacedRowsCount()).isEqualTo(200L);
    assertThat(copy.minSequenceNumber()).isEqualTo(5L);
    assertThat(copy.dvCardinality()).isEqualTo(1L);

    // verify deep copy of dv byte array
    assertThat(copy.dv().array()).isNotSameAs(info.dv().array());
  }

  @Test
  void testNullableFields() {
    ManifestInfoStruct info =
        ManifestInfoStruct.builder()
            .addedFilesCount(0)
            .existingFilesCount(0)
            .deletedFilesCount(0)
            .replacedFilesCount(0)
            .addedRowsCount(0L)
            .existingRowsCount(0L)
            .deletedRowsCount(0L)
            .replacedRowsCount(0L)
            .minSequenceNumber(0L)
            .build();

    assertThat(info.dv()).isNull();
    assertThat(info.dvCardinality()).isNull();
  }

  @Test
  void testProjectedStructLike() {
    // project only added_files_count (field ID 504) and min_sequence_number (field ID 516)
    Types.StructType projection =
        Types.StructType.of(ManifestInfo.ADDED_FILES_COUNT, ManifestInfo.MIN_SEQUENCE_NUMBER);

    ManifestInfoStruct info = new ManifestInfoStruct(projection);
    assertThat(info.size()).isEqualTo(2);

    // projected position 0 maps to internal position 0 (added_files_count)
    // projected position 1 maps to internal position 8 (min_sequence_number)
    info.set(0, 10);
    info.set(1, 5L);

    assertThat(info.addedFilesCount()).isEqualTo(10);
    assertThat(info.minSequenceNumber()).isEqualTo(5L);
    assertThat(info.get(0, Integer.class)).isEqualTo(10);
    assertThat(info.get(1, Long.class)).isEqualTo(5L);
  }

  @Test
  void testJavaSerializationRoundTrip() throws IOException, ClassNotFoundException {
    ManifestInfoStruct info =
        ManifestInfoStruct.builder()
            .addedFilesCount(10)
            .existingFilesCount(20)
            .deletedFilesCount(3)
            .replacedFilesCount(2)
            .addedRowsCount(1000L)
            .existingRowsCount(2000L)
            .deletedRowsCount(300L)
            .replacedRowsCount(200L)
            .minSequenceNumber(5L)
            .dv(new byte[] {0xF})
            .dvCardinality(1L)
            .build();

    ManifestInfoStruct deserialized = TestHelpers.roundTripSerialize(info);

    assertThat(deserialized.addedFilesCount()).isEqualTo(10);
    assertThat(deserialized.existingFilesCount()).isEqualTo(20);
    assertThat(deserialized.deletedFilesCount()).isEqualTo(3);
    assertThat(deserialized.replacedFilesCount()).isEqualTo(2);
    assertThat(deserialized.addedRowsCount()).isEqualTo(1000L);
    assertThat(deserialized.existingRowsCount()).isEqualTo(2000L);
    assertThat(deserialized.deletedRowsCount()).isEqualTo(300L);
    assertThat(deserialized.replacedRowsCount()).isEqualTo(200L);
    assertThat(deserialized.minSequenceNumber()).isEqualTo(5L);
    assertThat(deserialized.dv()).isEqualTo(ByteBuffer.wrap(new byte[] {0xF}));
    assertThat(deserialized.dvCardinality()).isEqualTo(1L);
  }

  @Test
  void testBuilderValidation() {
    assertThatThrownBy(
            () ->
                ManifestInfoStruct.builder()
                    .existingFilesCount(0)
                    .deletedFilesCount(0)
                    .replacedFilesCount(0)
                    .addedRowsCount(0L)
                    .existingRowsCount(0L)
                    .deletedRowsCount(0L)
                    .replacedRowsCount(0L)
                    .minSequenceNumber(0L)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid added files count: -1 (must be >= 0)");

    assertThatThrownBy(
            () ->
                ManifestInfoStruct.builder()
                    .addedFilesCount(0)
                    .deletedFilesCount(0)
                    .replacedFilesCount(0)
                    .addedRowsCount(0L)
                    .existingRowsCount(0L)
                    .deletedRowsCount(0L)
                    .replacedRowsCount(0L)
                    .minSequenceNumber(0L)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid existing files count: -1 (must be >= 0)");

    assertThatThrownBy(
            () ->
                ManifestInfoStruct.builder()
                    .addedFilesCount(0)
                    .existingFilesCount(0)
                    .replacedFilesCount(0)
                    .addedRowsCount(0L)
                    .existingRowsCount(0L)
                    .deletedRowsCount(0L)
                    .replacedRowsCount(0L)
                    .minSequenceNumber(0L)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid deleted files count: -1 (must be >= 0)");

    assertThatThrownBy(
            () ->
                ManifestInfoStruct.builder()
                    .addedFilesCount(0)
                    .existingFilesCount(0)
                    .deletedFilesCount(0)
                    .addedRowsCount(0L)
                    .existingRowsCount(0L)
                    .deletedRowsCount(0L)
                    .replacedRowsCount(0L)
                    .minSequenceNumber(0L)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid replaced files count: -1 (must be >= 0)");

    assertThatThrownBy(
            () ->
                ManifestInfoStruct.builder()
                    .addedFilesCount(0)
                    .existingFilesCount(0)
                    .deletedFilesCount(0)
                    .replacedFilesCount(0)
                    .existingRowsCount(0L)
                    .deletedRowsCount(0L)
                    .replacedRowsCount(0L)
                    .minSequenceNumber(0L)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid added rows count: -1 (must be >= 0)");

    assertThatThrownBy(
            () ->
                ManifestInfoStruct.builder()
                    .addedFilesCount(0)
                    .existingFilesCount(0)
                    .deletedFilesCount(0)
                    .replacedFilesCount(0)
                    .addedRowsCount(0L)
                    .deletedRowsCount(0L)
                    .replacedRowsCount(0L)
                    .minSequenceNumber(0L)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid existing rows count: -1 (must be >= 0)");

    assertThatThrownBy(
            () ->
                ManifestInfoStruct.builder()
                    .addedFilesCount(0)
                    .existingFilesCount(0)
                    .deletedFilesCount(0)
                    .replacedFilesCount(0)
                    .addedRowsCount(0L)
                    .existingRowsCount(0L)
                    .replacedRowsCount(0L)
                    .minSequenceNumber(0L)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid deleted rows count: -1 (must be >= 0)");

    assertThatThrownBy(
            () ->
                ManifestInfoStruct.builder()
                    .addedFilesCount(0)
                    .existingFilesCount(0)
                    .deletedFilesCount(0)
                    .replacedFilesCount(0)
                    .addedRowsCount(0L)
                    .existingRowsCount(0L)
                    .deletedRowsCount(0L)
                    .minSequenceNumber(0L)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid replaced rows count: -1 (must be >= 0)");

    assertThatThrownBy(
            () ->
                ManifestInfoStruct.builder()
                    .addedFilesCount(0)
                    .existingFilesCount(0)
                    .deletedFilesCount(0)
                    .replacedFilesCount(0)
                    .addedRowsCount(0L)
                    .existingRowsCount(0L)
                    .deletedRowsCount(0L)
                    .replacedRowsCount(0L)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid min sequence number: -1 (must be >= 0)");
  }

  @Test
  void testBuilderDvPairingValidation() {
    assertThatThrownBy(
            () ->
                ManifestInfoStruct.builder()
                    .addedFilesCount(0)
                    .existingFilesCount(0)
                    .deletedFilesCount(0)
                    .replacedFilesCount(0)
                    .addedRowsCount(0L)
                    .existingRowsCount(0L)
                    .deletedRowsCount(0L)
                    .replacedRowsCount(0L)
                    .minSequenceNumber(0L)
                    .dv(new byte[] {0xF})
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid DV and cardinality: must both be null or non-null");

    assertThatThrownBy(
            () ->
                ManifestInfoStruct.builder()
                    .addedFilesCount(0)
                    .existingFilesCount(0)
                    .deletedFilesCount(0)
                    .replacedFilesCount(0)
                    .addedRowsCount(0L)
                    .existingRowsCount(0L)
                    .deletedRowsCount(0L)
                    .replacedRowsCount(0L)
                    .minSequenceNumber(0L)
                    .dvCardinality(1L)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid DV and cardinality: must both be null or non-null");

    assertThatThrownBy(
            () ->
                ManifestInfoStruct.builder()
                    .addedFilesCount(0)
                    .existingFilesCount(0)
                    .deletedFilesCount(0)
                    .replacedFilesCount(0)
                    .addedRowsCount(0L)
                    .existingRowsCount(0L)
                    .deletedRowsCount(0L)
                    .replacedRowsCount(0L)
                    .minSequenceNumber(0L)
                    .dv(new byte[] {0xF})
                    .dvCardinality(0L)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid DV cardinality: 0 (must be positive)");
  }

  @Test
  void testKryoSerializationRoundTrip() throws IOException {
    ManifestInfoStruct info =
        ManifestInfoStruct.builder()
            .addedFilesCount(10)
            .existingFilesCount(20)
            .deletedFilesCount(3)
            .replacedFilesCount(2)
            .addedRowsCount(1000L)
            .existingRowsCount(2000L)
            .deletedRowsCount(300L)
            .replacedRowsCount(200L)
            .minSequenceNumber(5L)
            .dv(new byte[] {0xF})
            .dvCardinality(1L)
            .build();

    ManifestInfoStruct deserialized = TestHelpers.KryoHelpers.roundTripSerialize(info);

    assertThat(deserialized.addedFilesCount()).isEqualTo(10);
    assertThat(deserialized.existingFilesCount()).isEqualTo(20);
    assertThat(deserialized.deletedFilesCount()).isEqualTo(3);
    assertThat(deserialized.replacedFilesCount()).isEqualTo(2);
    assertThat(deserialized.addedRowsCount()).isEqualTo(1000L);
    assertThat(deserialized.existingRowsCount()).isEqualTo(2000L);
    assertThat(deserialized.deletedRowsCount()).isEqualTo(300L);
    assertThat(deserialized.replacedRowsCount()).isEqualTo(200L);
    assertThat(deserialized.minSequenceNumber()).isEqualTo(5L);
    assertThat(deserialized.dv()).isEqualTo(ByteBuffer.wrap(new byte[] {0xF}));
    assertThat(deserialized.dvCardinality()).isEqualTo(1L);
  }
}
