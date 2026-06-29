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
    ManifestInfoStruct info =
        new ManifestInfoStruct(10, 20, 3, 2, 1000L, 2000L, 300L, 200L, 5L, new byte[] {0xF}, 1L);

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
            .dv(ByteBuffer.wrap(new byte[] {0xF}))
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
  void testInternalSetIgnoresUnknownOrdinal() {
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
            .dv(ByteBuffer.wrap(new byte[] {0xF}))
            .dvCardinality(1L)
            .build();

    // unknown ordinals from a newer format version are silently ignored
    info.internalSet(99, "value from a newer format");

    // every field is unchanged
    assertThat(info.addedFilesCount()).isEqualTo(10);
    assertThat(info.existingFilesCount()).isEqualTo(20);
    assertThat(info.deletedFilesCount()).isEqualTo(3);
    assertThat(info.replacedFilesCount()).isEqualTo(2);
    assertThat(info.addedRowsCount()).isEqualTo(1000L);
    assertThat(info.existingRowsCount()).isEqualTo(2000L);
    assertThat(info.deletedRowsCount()).isEqualTo(300L);
    assertThat(info.replacedRowsCount()).isEqualTo(200L);
    assertThat(info.minSequenceNumber()).isEqualTo(5L);
    assertThat(info.dv()).isEqualTo(ByteBuffer.wrap(new byte[] {0xF}));
    assertThat(info.dvCardinality()).isEqualTo(1L);
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
            .dv(ByteBuffer.wrap(new byte[] {0xF}))
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
  void testBuilderMissingAddedFilesCount() {
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
        .hasMessage("Missing required value: added files count");
  }

  @Test
  void testBuilderMissingExistingFilesCount() {
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
        .hasMessage("Missing required value: existing files count");
  }

  @Test
  void testBuilderMissingDeletedFilesCount() {
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
        .hasMessage("Missing required value: deleted files count");
  }

  @Test
  void testBuilderMissingReplacedFilesCount() {
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
        .hasMessage("Missing required value: replaced files count");
  }

  @Test
  void testBuilderMissingAddedRowsCount() {
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
        .hasMessage("Missing required value: added rows count");
  }

  @Test
  void testBuilderMissingExistingRowsCount() {
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
        .hasMessage("Missing required value: existing rows count");
  }

  @Test
  void testBuilderMissingDeletedRowsCount() {
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
        .hasMessage("Missing required value: deleted rows count");
  }

  @Test
  void testBuilderMissingReplacedRowsCount() {
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
        .hasMessage("Missing required value: replaced rows count");
  }

  @Test
  void testBuilderMissingMinSequenceNumber() {
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
        .hasMessage("Missing required value: min sequence number");
  }

  @Test
  void testBuilderRejectsNegativeAddedFilesCount() {
    assertThatThrownBy(() -> ManifestInfoStruct.builder().addedFilesCount(-1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid added files count: -1 (must be >= 0)");
  }

  @Test
  void testBuilderRejectsNegativeExistingFilesCount() {
    assertThatThrownBy(() -> ManifestInfoStruct.builder().existingFilesCount(-1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid existing files count: -1 (must be >= 0)");
  }

  @Test
  void testBuilderRejectsNegativeDeletedFilesCount() {
    assertThatThrownBy(() -> ManifestInfoStruct.builder().deletedFilesCount(-1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid deleted files count: -1 (must be >= 0)");
  }

  @Test
  void testBuilderRejectsNegativeReplacedFilesCount() {
    assertThatThrownBy(() -> ManifestInfoStruct.builder().replacedFilesCount(-1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid replaced files count: -1 (must be >= 0)");
  }

  @Test
  void testBuilderRejectsNegativeAddedRowsCount() {
    assertThatThrownBy(() -> ManifestInfoStruct.builder().addedRowsCount(-1L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid added rows count: -1 (must be >= 0)");
  }

  @Test
  void testBuilderRejectsNegativeExistingRowsCount() {
    assertThatThrownBy(() -> ManifestInfoStruct.builder().existingRowsCount(-1L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid existing rows count: -1 (must be >= 0)");
  }

  @Test
  void testBuilderRejectsNegativeDeletedRowsCount() {
    assertThatThrownBy(() -> ManifestInfoStruct.builder().deletedRowsCount(-1L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid deleted rows count: -1 (must be >= 0)");
  }

  @Test
  void testBuilderRejectsNegativeReplacedRowsCount() {
    assertThatThrownBy(() -> ManifestInfoStruct.builder().replacedRowsCount(-1L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid replaced rows count: -1 (must be >= 0)");
  }

  @Test
  void testBuilderRejectsNegativeMinSequenceNumber() {
    assertThatThrownBy(() -> ManifestInfoStruct.builder().minSequenceNumber(-1L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid min sequence number: -1 (must be >= 0)");
  }

  @Test
  void testBuilderRejectsNegativeDvCardinality() {
    assertThatThrownBy(() -> ManifestInfoStruct.builder().dvCardinality(-1L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid DV cardinality: -1 (must be >= 0)");
  }

  @Test
  void testBuilderRejectsRowsWithoutFiles() {
    assertThatThrownBy(
            () ->
                ManifestInfoStruct.builder()
                    .addedFilesCount(0)
                    .existingFilesCount(0)
                    .deletedFilesCount(0)
                    .replacedFilesCount(0)
                    .addedRowsCount(10L)
                    .existingRowsCount(0L)
                    .deletedRowsCount(0L)
                    .replacedRowsCount(0L)
                    .minSequenceNumber(0L)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid added counts: 10 rows in 0 files");

    assertThatThrownBy(
            () ->
                ManifestInfoStruct.builder()
                    .addedFilesCount(0)
                    .existingFilesCount(0)
                    .deletedFilesCount(0)
                    .replacedFilesCount(0)
                    .addedRowsCount(0L)
                    .existingRowsCount(5L)
                    .deletedRowsCount(0L)
                    .replacedRowsCount(0L)
                    .minSequenceNumber(0L)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid existing counts: 5 rows in 0 files");

    assertThatThrownBy(
            () ->
                ManifestInfoStruct.builder()
                    .addedFilesCount(0)
                    .existingFilesCount(0)
                    .deletedFilesCount(0)
                    .replacedFilesCount(0)
                    .addedRowsCount(0L)
                    .existingRowsCount(0L)
                    .deletedRowsCount(3L)
                    .replacedRowsCount(0L)
                    .minSequenceNumber(0L)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid deleted counts: 3 rows in 0 files");

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
                    .replacedRowsCount(7L)
                    .minSequenceNumber(0L)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid replaced counts: 7 rows in 0 files");
  }

  @Test
  void testBuilderAllowsFilesWithoutRows() {
    ManifestInfoStruct info =
        ManifestInfoStruct.builder()
            .addedFilesCount(5)
            .existingFilesCount(5)
            .deletedFilesCount(5)
            .replacedFilesCount(5)
            .addedRowsCount(0L)
            .existingRowsCount(0L)
            .deletedRowsCount(0L)
            .replacedRowsCount(0L)
            .minSequenceNumber(0L)
            .build();

    assertThat(info.addedFilesCount()).isEqualTo(5);
    assertThat(info.existingFilesCount()).isEqualTo(5);
    assertThat(info.deletedFilesCount()).isEqualTo(5);
    assertThat(info.replacedFilesCount()).isEqualTo(5);
    assertThat(info.addedRowsCount()).isEqualTo(0L);
    assertThat(info.existingRowsCount()).isEqualTo(0L);
    assertThat(info.deletedRowsCount()).isEqualTo(0L);
    assertThat(info.replacedRowsCount()).isEqualTo(0L);
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
                    .dv(ByteBuffer.wrap(new byte[] {0xF}))
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
            .dv(ByteBuffer.wrap(new byte[] {0xF}))
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
