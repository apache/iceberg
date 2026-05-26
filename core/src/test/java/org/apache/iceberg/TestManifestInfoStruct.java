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
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TestManifestInfoStruct {

  // Ordinals looked up from ManifestInfo.schema() so tests don't hard-code positions.
  private static final List<Types.NestedField> INFO_FIELDS = ManifestInfo.schema().fields();
  private static final int ADDED_FILES_COUNT_ORDINAL =
      INFO_FIELDS.indexOf(ManifestInfo.ADDED_FILES_COUNT);
  private static final int EXISTING_FILES_COUNT_ORDINAL =
      INFO_FIELDS.indexOf(ManifestInfo.EXISTING_FILES_COUNT);
  private static final int DELETED_FILES_COUNT_ORDINAL =
      INFO_FIELDS.indexOf(ManifestInfo.DELETED_FILES_COUNT);
  private static final int REPLACED_FILES_COUNT_ORDINAL =
      INFO_FIELDS.indexOf(ManifestInfo.REPLACED_FILES_COUNT);
  private static final int ADDED_ROWS_COUNT_ORDINAL =
      INFO_FIELDS.indexOf(ManifestInfo.ADDED_ROWS_COUNT);
  private static final int EXISTING_ROWS_COUNT_ORDINAL =
      INFO_FIELDS.indexOf(ManifestInfo.EXISTING_ROWS_COUNT);
  private static final int DELETED_ROWS_COUNT_ORDINAL =
      INFO_FIELDS.indexOf(ManifestInfo.DELETED_ROWS_COUNT);
  private static final int REPLACED_ROWS_COUNT_ORDINAL =
      INFO_FIELDS.indexOf(ManifestInfo.REPLACED_ROWS_COUNT);
  private static final int MIN_SEQUENCE_NUMBER_ORDINAL =
      INFO_FIELDS.indexOf(ManifestInfo.MIN_SEQUENCE_NUMBER);
  private static final int DV_ORDINAL = INFO_FIELDS.indexOf(ManifestInfo.DV);
  private static final int DV_CARDINALITY_ORDINAL =
      INFO_FIELDS.indexOf(ManifestInfo.DV_CARDINALITY);

  @Test
  void testFieldAccess() {
    ManifestInfoStruct info = new ManifestInfoStruct(ManifestInfo.schema());

    info.set(ADDED_FILES_COUNT_ORDINAL, 10);
    info.set(EXISTING_FILES_COUNT_ORDINAL, 20);
    info.set(DELETED_FILES_COUNT_ORDINAL, 3);
    info.set(REPLACED_FILES_COUNT_ORDINAL, 2);
    info.set(ADDED_ROWS_COUNT_ORDINAL, 1000L);
    info.set(EXISTING_ROWS_COUNT_ORDINAL, 2000L);
    info.set(DELETED_ROWS_COUNT_ORDINAL, 300L);
    info.set(REPLACED_ROWS_COUNT_ORDINAL, 200L);
    info.set(MIN_SEQUENCE_NUMBER_ORDINAL, 5L);
    info.set(DV_ORDINAL, ByteBuffer.wrap(new byte[] {0xF}));
    info.set(DV_CARDINALITY_ORDINAL, 1L);

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

  // Keyed by the field name used in the "Missing required value: ..." build() error so each
  // case has a single source of truth. No dependency on schema field order.
  private static final Map<String, Consumer<ManifestInfoStruct.Builder>> REQUIRED_SETTERS =
      Map.ofEntries(
          Map.entry("added files count", b -> b.addedFilesCount(0)),
          Map.entry("existing files count", b -> b.existingFilesCount(0)),
          Map.entry("deleted files count", b -> b.deletedFilesCount(0)),
          Map.entry("replaced files count", b -> b.replacedFilesCount(0)),
          Map.entry("added rows count", b -> b.addedRowsCount(0L)),
          Map.entry("existing rows count", b -> b.existingRowsCount(0L)),
          Map.entry("deleted rows count", b -> b.deletedRowsCount(0L)),
          Map.entry("replaced rows count", b -> b.replacedRowsCount(0L)),
          Map.entry("min sequence number", b -> b.minSequenceNumber(0L)));

  private static Stream<String> missingRequiredFieldCases() {
    return REQUIRED_SETTERS.keySet().stream();
  }

  @ParameterizedTest
  @MethodSource("missingRequiredFieldCases")
  void testBuilderMissingRequiredFields(String missingField) {
    ManifestInfoStruct.Builder builder = ManifestInfoStruct.builder();
    REQUIRED_SETTERS.forEach(
        (name, setter) -> {
          if (!name.equals(missingField)) {
            setter.accept(builder);
          }
        });

    assertThatThrownBy(builder::build)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Missing required value: " + missingField);
  }

  private static Stream<Arguments> negativeValueAtSetterCases() {
    return Stream.of(
        Arguments.of(
            (Consumer<ManifestInfoStruct.Builder>) b -> b.addedFilesCount(-1),
            "Invalid added files count: -1 (must be >= 0)"),
        Arguments.of(
            (Consumer<ManifestInfoStruct.Builder>) b -> b.existingFilesCount(-1),
            "Invalid existing files count: -1 (must be >= 0)"),
        Arguments.of(
            (Consumer<ManifestInfoStruct.Builder>) b -> b.deletedFilesCount(-1),
            "Invalid deleted files count: -1 (must be >= 0)"),
        Arguments.of(
            (Consumer<ManifestInfoStruct.Builder>) b -> b.replacedFilesCount(-1),
            "Invalid replaced files count: -1 (must be >= 0)"),
        Arguments.of(
            (Consumer<ManifestInfoStruct.Builder>) b -> b.addedRowsCount(-1L),
            "Invalid added rows count: -1 (must be >= 0)"),
        Arguments.of(
            (Consumer<ManifestInfoStruct.Builder>) b -> b.existingRowsCount(-1L),
            "Invalid existing rows count: -1 (must be >= 0)"),
        Arguments.of(
            (Consumer<ManifestInfoStruct.Builder>) b -> b.deletedRowsCount(-1L),
            "Invalid deleted rows count: -1 (must be >= 0)"),
        Arguments.of(
            (Consumer<ManifestInfoStruct.Builder>) b -> b.replacedRowsCount(-1L),
            "Invalid replaced rows count: -1 (must be >= 0)"),
        Arguments.of(
            (Consumer<ManifestInfoStruct.Builder>) b -> b.minSequenceNumber(-1L),
            "Invalid min sequence number: -1 (must be >= 0)"),
        Arguments.of(
            (Consumer<ManifestInfoStruct.Builder>) b -> b.dvCardinality(0L),
            "Invalid DV cardinality: 0 (must be positive)"));
  }

  @ParameterizedTest
  @MethodSource("negativeValueAtSetterCases")
  void testBuilderRejectsNegativeValuesAtSetter(
      Consumer<ManifestInfoStruct.Builder> apply, String expectedMessage) {
    assertThatThrownBy(() -> apply.accept(ManifestInfoStruct.builder()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(expectedMessage);
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
