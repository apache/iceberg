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

import com.fasterxml.jackson.databind.JsonNode;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Stream;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.JsonUtil;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class TestContentFileParser {
  @Test
  public void testNullArguments() throws Exception {
    Assertions.assertThatThrownBy(() -> ContentFileParser.toJson(null, TableTestBase.SPEC))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid content file: null");

    Assertions.assertThatThrownBy(() -> ContentFileParser.toJson(TableTestBase.FILE_A, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid partition spec: null");

    Assertions.assertThatThrownBy(
            () -> ContentFileParser.toJson(TableTestBase.FILE_A, TableTestBase.SPEC, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid JSON generator: null");

    Assertions.assertThatThrownBy(() -> ContentFileParser.fromJson(null, TableTestBase.SPEC))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid JSON node for content file: null");

    String jsonStr = ContentFileParser.toJson(TableTestBase.FILE_A, TableTestBase.SPEC);
    JsonNode jsonNode = JsonUtil.mapper().readTree(jsonStr);
    Assertions.assertThatThrownBy(() -> ContentFileParser.fromJson(jsonNode, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid partition spec: null");
  }

  @ParameterizedTest
  @MethodSource("provideSpecAndDataFile")
  public void testDataFile(PartitionSpec spec, DataFile dataFile) throws Exception {
    String jsonStr = ContentFileParser.toJson(dataFile, spec);
    JsonNode jsonNode = JsonUtil.mapper().readTree(jsonStr);
    ContentFile<?> deserializedContentFile = ContentFileParser.fromJson(jsonNode, spec);
    Assertions.assertThat(deserializedContentFile).isInstanceOf(DataFile.class);
    assertContentFileEquals(dataFile, deserializedContentFile, spec);
  }

  @ParameterizedTest
  @MethodSource("provideSpecAndDeleteFile")
  public void testDeleteFile(PartitionSpec spec, DeleteFile deleteFile) throws Exception {
    String jsonStr = ContentFileParser.toJson(deleteFile, spec);
    JsonNode jsonNode = JsonUtil.mapper().readTree(jsonStr);
    ContentFile<?> deserializedContentFile = ContentFileParser.fromJson(jsonNode, spec);
    Assertions.assertThat(deserializedContentFile).isInstanceOf(DeleteFile.class);
    assertContentFileEquals(deleteFile, deserializedContentFile, spec);
  }

  private static Stream<Arguments> provideSpecAndDataFile() {
    return Stream.of(
        Arguments.of(
            PartitionSpec.unpartitioned(), dataFileWithRequiredOnly(PartitionSpec.unpartitioned())),
        Arguments.of(
            PartitionSpec.unpartitioned(), dataFileWithAllOptional(PartitionSpec.unpartitioned())),
        Arguments.of(TableTestBase.SPEC, dataFileWithRequiredOnly(TableTestBase.SPEC)),
        Arguments.of(TableTestBase.SPEC, dataFileWithAllOptional(TableTestBase.SPEC)));
  }

  private static DataFile dataFileWithRequiredOnly(PartitionSpec spec) {
    DataFiles.Builder builder =
        DataFiles.builder(spec)
            .withPath("/path/to/data-a.parquet")
            .withFileSizeInBytes(10)
            .withRecordCount(1);

    if (spec.isPartitioned()) {
      // easy way to set partition data for now
      builder.withPartitionPath("data_bucket=1");
    }

    return builder.build();
  }

  private static DataFile dataFileWithAllOptional(PartitionSpec spec) {
    DataFiles.Builder builder =
        DataFiles.builder(spec)
            .withPath("/path/to/data-with-stats.parquet")
            .withMetrics(
                new Metrics(
                    10L, // record count
                    ImmutableMap.of(3, 100L, 4, 200L), // column sizes
                    ImmutableMap.of(3, 90L, 4, 180L), // value counts
                    ImmutableMap.of(3, 10L, 4, 20L), // null value counts
                    ImmutableMap.of(3, 0L, 4, 0L), // nan value counts
                    ImmutableMap.of(
                        3,
                        Conversions.toByteBuffer(Types.IntegerType.get(), 1),
                        4,
                        Conversions.toByteBuffer(Types.IntegerType.get(), 2)), // lower bounds
                    ImmutableMap.of(
                        3,
                        Conversions.toByteBuffer(Types.IntegerType.get(), 5),
                        4,
                        Conversions.toByteBuffer(Types.IntegerType.get(), 10)) // upperbounds
                    ))
            .withFileSizeInBytes(350)
            .withSplitOffsets(Arrays.asList(128L, 256L))
            .withEqualityFieldIds(Collections.singletonList(1))
            .withEncryptionKeyMetadata(ByteBuffer.wrap(new byte[16]))
            .withSortOrder(
                SortOrder.builderFor(TableTestBase.SCHEMA)
                    .withOrderId(1)
                    .sortBy("id", SortDirection.ASC, NullOrder.NULLS_FIRST)
                    .build());

    if (spec.isPartitioned()) {
      // easy way to set partition data for now
      builder.withPartitionPath("data_bucket=1");
    }

    return builder.build();
  }

  private static Stream<Arguments> provideSpecAndDeleteFile() {
    return Stream.of(
        Arguments.of(
            PartitionSpec.unpartitioned(),
            deleteFileWithRequiredOnly(PartitionSpec.unpartitioned())),
        Arguments.of(
            PartitionSpec.unpartitioned(),
            deleteFileWithAllOptional(PartitionSpec.unpartitioned())),
        Arguments.of(TableTestBase.SPEC, deleteFileWithRequiredOnly(TableTestBase.SPEC)),
        Arguments.of(TableTestBase.SPEC, deleteFileWithAllOptional(TableTestBase.SPEC)));
  }

  private static DeleteFile deleteFileWithRequiredOnly(PartitionSpec spec) {
    PartitionData partitionData = null;
    if (spec.isPartitioned()) {
      partitionData = new PartitionData(spec.partitionType());
      partitionData.set(0, 9);
    }

    return new GenericDeleteFile(
        spec.specId(),
        FileContent.POSITION_DELETES,
        "/path/to/delete-a.parquet",
        FileFormat.PARQUET,
        partitionData,
        1234,
        new Metrics(9L, null, null, null, null),
        null,
        null,
        null,
        null);
  }

  private static DeleteFile deleteFileWithAllOptional(PartitionSpec spec) {
    PartitionData partitionData = new PartitionData(spec.partitionType());
    if (spec.isPartitioned()) {
      partitionData.set(0, 9);
    }

    Metrics metrics =
        new Metrics(
            10L, // record count
            ImmutableMap.of(3, 100L, 4, 200L), // column sizes
            ImmutableMap.of(3, 90L, 4, 180L), // value counts
            ImmutableMap.of(3, 10L, 4, 20L), // null value counts
            ImmutableMap.of(3, 0L, 4, 0L), // nan value counts
            ImmutableMap.of(
                3,
                Conversions.toByteBuffer(Types.IntegerType.get(), 1),
                4,
                Conversions.toByteBuffer(Types.IntegerType.get(), 2)), // lower bounds
            ImmutableMap.of(
                3,
                Conversions.toByteBuffer(Types.IntegerType.get(), 5),
                4,
                Conversions.toByteBuffer(Types.IntegerType.get(), 10)) // upperbounds
            );

    return new GenericDeleteFile(
        spec.specId(),
        FileContent.EQUALITY_DELETES,
        "/path/to/delete-with-stats.parquet",
        FileFormat.PARQUET,
        partitionData,
        1234,
        metrics,
        new int[] {3},
        1,
        Arrays.asList(128L),
        ByteBuffer.wrap(new byte[16]));
  }

  static void assertContentFileEquals(
      ContentFile<?> expected, ContentFile<?> actual, PartitionSpec spec) {
    Assertions.assertThat(actual.getClass()).isEqualTo(expected.getClass());
    Assertions.assertThat(actual.specId()).isEqualTo(expected.specId());
    Assertions.assertThat(actual.content()).isEqualTo(expected.content());
    Assertions.assertThat(actual.path()).isEqualTo(expected.path());
    Assertions.assertThat(actual.format()).isEqualTo(expected.format());
    Assertions.assertThat(actual.partition())
        .usingComparator(Comparators.forType(spec.partitionType()))
        .isEqualTo(expected.partition());
    Assertions.assertThat(actual.recordCount()).isEqualTo(expected.recordCount());
    Assertions.assertThat(actual.fileSizeInBytes()).isEqualTo(expected.fileSizeInBytes());
    Assertions.assertThat(actual.columnSizes()).isEqualTo(expected.columnSizes());
    Assertions.assertThat(actual.valueCounts()).isEqualTo(expected.valueCounts());
    Assertions.assertThat(actual.nullValueCounts()).isEqualTo(expected.nullValueCounts());
    Assertions.assertThat(actual.nanValueCounts()).isEqualTo(expected.nanValueCounts());
    Assertions.assertThat(actual.lowerBounds()).isEqualTo(expected.lowerBounds());
    Assertions.assertThat(actual.upperBounds()).isEqualTo(expected.upperBounds());
    Assertions.assertThat(actual.keyMetadata()).isEqualTo(expected.keyMetadata());
    Assertions.assertThat(actual.splitOffsets()).isEqualTo(expected.splitOffsets());
    Assertions.assertThat(actual.equalityFieldIds()).isEqualTo(expected.equalityFieldIds());
    Assertions.assertThat(actual.sortOrderId()).isEqualTo(expected.sortOrderId());
  }
}
