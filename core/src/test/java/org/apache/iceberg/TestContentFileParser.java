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

import com.fasterxml.jackson.databind.JsonNode;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.stats.BaseContentStats;
import org.apache.iceberg.stats.BaseFieldStats;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.JsonUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class TestContentFileParser {
  @Test
  public void testNullArguments() throws Exception {
    assertThatThrownBy(() -> ContentFileParser.toJson(null, TestBase.SPEC))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid content file: null");

    assertThatThrownBy(() -> ContentFileParser.toJson(TestBase.FILE_A, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid partition spec: null");

    assertThatThrownBy(() -> ContentFileParser.toJson(TestBase.FILE_A, TestBase.SPEC, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid JSON generator: null");

    assertThatThrownBy(
            () -> ContentFileParser.fromJson(null, Map.of(TestBase.SPEC.specId(), TestBase.SPEC)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid JSON node for content file: null");

    String jsonStr = ContentFileParser.toJson(TestBase.FILE_A, TestBase.SPEC);
    JsonNode jsonNode = JsonUtil.mapper().readTree(jsonStr);
    assertThatThrownBy(() -> ContentFileParser.fromJson(jsonNode, (PartitionSpec) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid partition spec: null");
  }

  @ParameterizedTest
  @MethodSource("provideSpecAndDataFile")
  public void testDataFile(PartitionSpec spec, DataFile dataFile, String expectedJson)
      throws Exception {
    String jsonStr = ContentFileParser.toJson(dataFile, spec);
    assertThat(jsonStr).isEqualTo(expectedJson);
    JsonNode jsonNode = JsonUtil.mapper().readTree(jsonStr);
    ContentFile<?> deserializedContentFile =
        ContentFileParser.fromJson(jsonNode, Map.of(TestBase.SPEC.specId(), spec));
    assertThat(deserializedContentFile).isInstanceOf(DataFile.class);
    assertContentFileEquals(dataFile, deserializedContentFile, spec);
  }

  @ParameterizedTest
  @MethodSource("provideSpecAndDeleteFile")
  public void testDeleteFile(PartitionSpec spec, DeleteFile deleteFile, String expectedJson)
      throws Exception {
    String jsonStr = ContentFileParser.toJson(deleteFile, spec);
    assertThat(jsonStr).isEqualTo(expectedJson);
    JsonNode jsonNode = JsonUtil.mapper().readTree(jsonStr);
    ContentFile<?> deserializedContentFile =
        ContentFileParser.fromJson(jsonNode, Map.of(spec.specId(), TestBase.SPEC));
    assertThat(deserializedContentFile).isInstanceOf(DeleteFile.class);
    assertContentFileEquals(deleteFile, deserializedContentFile, spec);
  }

  @ParameterizedTest
  @MethodSource("provideContentStatsAndDataFile")
  public void testDataFileContentStats(PartitionSpec spec, DataFile dataFile, String expectedJson)
      throws Exception {
    String jsonStr = ContentFileParser.toJson(dataFile, spec);
    assertThat(jsonStr).isEqualTo(expectedJson);
    JsonNode jsonNode = JsonUtil.mapper().readTree(jsonStr);
    ContentFile<?> deserializedContentFile =
        ContentFileParser.fromJson(jsonNode, Map.of(TestBase.SPEC.specId(), spec));
    assertThat(deserializedContentFile).isInstanceOf(DataFile.class);
    assertContentFileEquals(dataFile, deserializedContentFile, spec);
  }

  private static Stream<Arguments> provideContentStatsAndDataFile() {
    return Stream.of(
        Arguments.of(
            TestBase.SPEC,
            dataFileWithContentStats(TestBase.SPEC, true, true, true, true, true),
            dataFileJsonWithContentStats(true, true, true, true, true)),
        Arguments.of(
            TestBase.SPEC,
            dataFileWithContentStats(TestBase.SPEC, false, true, true, true, true),
            dataFileJsonWithContentStats(false, true, true, true, true)),
        Arguments.of(
            TestBase.SPEC,
            dataFileWithContentStats(TestBase.SPEC, false, false, true, true, true),
            dataFileJsonWithContentStats(false, false, true, true, true)),
        Arguments.of(
            TestBase.SPEC,
            dataFileWithContentStats(TestBase.SPEC, false, false, false, true, true),
            dataFileJsonWithContentStats(false, false, false, true, true)),
        Arguments.of(
            TestBase.SPEC,
            dataFileWithContentStats(TestBase.SPEC, false, false, false, false, true),
            dataFileJsonWithContentStats(false, false, false, false, true)),
        Arguments.of(
            TestBase.SPEC,
            dataFileWithContentStats(TestBase.SPEC, false, false, false, false, false),
            dataFileJsonWithContentStats(false, false, false, false, false)),
        Arguments.of(
            TestBase.SPEC,
            dataFileWithContentStats(TestBase.SPEC, true, false, false, true, true),
            dataFileJsonWithContentStats(true, false, false, true, true)),
        Arguments.of(
            TestBase.SPEC,
            dataFileWithContentStats(TestBase.SPEC, true, false, true, false, true),
            dataFileJsonWithContentStats(true, false, true, false, true)),
        Arguments.of(
            TestBase.SPEC,
            dataFileWithContentStats(TestBase.SPEC, true, true, true, false, false),
            dataFileJsonWithContentStats(true, true, true, false, false)));
  }

  private static Stream<Arguments> provideSpecAndDataFile() {
    return Stream.of(
        Arguments.of(
            PartitionSpec.unpartitioned(),
            dataFileWithRequiredOnly(PartitionSpec.unpartitioned()),
            dataFileJsonWithRequiredOnly(PartitionSpec.unpartitioned())),
        Arguments.of(
            PartitionSpec.unpartitioned(),
            dataFileWithAllOptional(PartitionSpec.unpartitioned()),
            dataFileJsonWithAllOptional(PartitionSpec.unpartitioned())),
        Arguments.of(
            TestBase.SPEC,
            dataFileWithRequiredOnly(TestBase.SPEC),
            dataFileJsonWithRequiredOnly(TestBase.SPEC)),
        Arguments.of(
            TestBase.SPEC,
            dataFileWithAllOptional(TestBase.SPEC),
            dataFileJsonWithAllOptional(TestBase.SPEC)));
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

  private static String dataFileJsonWithRequiredOnly(PartitionSpec spec) {
    if (spec.isUnpartitioned()) {
      return "{\"spec-id\":0,\"content\":\"DATA\",\"file-path\":\"/path/to/data-a.parquet\",\"file-format\":\"PARQUET\","
          + "\"partition\":{},\"file-size-in-bytes\":10,\"record-count\":1,\"sort-order-id\":0}";
    } else {
      return "{\"spec-id\":0,\"content\":\"DATA\",\"file-path\":\"/path/to/data-a.parquet\",\"file-format\":\"PARQUET\","
          + "\"partition\":{\"1000\":1},\"file-size-in-bytes\":10,\"record-count\":1,\"sort-order-id\":0}";
    }
  }

  private static String dataFileJsonWithAllOptional(PartitionSpec spec) {
    if (spec.isUnpartitioned()) {
      return "{\"spec-id\":0,\"content\":\"DATA\",\"file-path\":\"/path/to/data-with-stats.parquet\","
          + "\"file-format\":\"PARQUET\",\"partition\":{},\"file-size-in-bytes\":350,\"record-count\":10,"
          + "\"column-sizes\":{\"keys\":[3,4],\"values\":[100,200]},"
          + "\"value-counts\":{\"keys\":[3,4],\"values\":[90,180]},"
          + "\"null-value-counts\":{\"keys\":[3,4],\"values\":[10,20]},"
          + "\"nan-value-counts\":{\"keys\":[3,4],\"values\":[0,0]},"
          + "\"lower-bounds\":{\"keys\":[3,4],\"values\":[\"01000000\",\"02000000\"]},"
          + "\"upper-bounds\":{\"keys\":[3,4],\"values\":[\"05000000\",\"0A000000\"]},"
          + "\"key-metadata\":\"00000000000000000000000000000000\","
          + "\"split-offsets\":[128,256],\"sort-order-id\":1}";
    } else {
      return "{\"spec-id\":0,\"content\":\"DATA\",\"file-path\":\"/path/to/data-with-stats.parquet\","
          + "\"file-format\":\"PARQUET\",\"partition\":{\"1000\":1},\"file-size-in-bytes\":350,\"record-count\":10,"
          + "\"column-sizes\":{\"keys\":[3,4],\"values\":[100,200]},"
          + "\"value-counts\":{\"keys\":[3,4],\"values\":[90,180]},"
          + "\"null-value-counts\":{\"keys\":[3,4],\"values\":[10,20]},"
          + "\"nan-value-counts\":{\"keys\":[3,4],\"values\":[0,0]},"
          + "\"lower-bounds\":{\"keys\":[3,4],\"values\":[\"01000000\",\"02000000\"]},"
          + "\"upper-bounds\":{\"keys\":[3,4],\"values\":[\"05000000\",\"0A000000\"]},"
          + "\"key-metadata\":\"00000000000000000000000000000000\","
          + "\"split-offsets\":[128,256],\"sort-order-id\":1}";
    }
  }

  private static String dataFileJsonWithContentStats(
      boolean hasValueCount,
      boolean hasNullValueCount,
      boolean hasNanValueCount,
      boolean hasLowBound,
      boolean hasUpperBound) {
    StringBuilder sb = new StringBuilder();
    sb.append(
            "{\"spec-id\":0,\"content\":\"DATA\",\"file-path\":\"/path/to/data-with-stats.parquet\",")
        .append("\"file-format\":\"PARQUET\",")
        .append("\"partition\":{\"1000\":1},")
        .append("\"file-size-in-bytes\":350,")
        .append("\"record-count\":10,")
        .append("\"key-metadata\":\"00000000000000000000000000000000\",")
        .append("\"split-offsets\":[128,256],")
        .append("\"sort-order-id\":1,")
        .append("\"content-stats\":{");

    // Use a loop to reduce repetition in serializing field stats
    int[] fieldIds = {10200, 10400};
    Object[][] fieldValues = {
      // int field: field id, valueCount, nullValueCount, nanValueCount, lowerBound, upperBound
      {90, 10, 0, 1000000, 5000000},
      // string field: field id, valueCount, nullValueCount, nanValueCount, lowerBound, upperBound
      {180, 20, 0, "02000000", "0A000000"}
    };

    for (int i = 0; i < fieldIds.length; i++) {
      sb.append("\"").append(fieldIds[i]).append("\":{");
      boolean first = true;

      // value count
      if (hasValueCount) {
        sb.append("\"").append(fieldIds[i] + 2).append("\":").append(fieldValues[i][0]);
        first = false;
      }
      // null value count
      if (hasNullValueCount) {
        if (!first) sb.append(",");
        sb.append("\"").append(fieldIds[i] + 3).append("\":").append(fieldValues[i][1]);
        first = false;
      }
      // nan value count
      if (hasNanValueCount) {
        if (!first) sb.append(",");
        sb.append("\"").append(fieldIds[i] + 4).append("\":").append(fieldValues[i][2]);
        first = false;
      }
      // lower bound
      if (hasLowBound) {
        if (!first) sb.append(",");
        boolean isStringField = (i == 1);
        sb.append("\"").append(fieldIds[i] + 5).append("\":");
        if (isStringField) {
          sb.append("\"").append(fieldValues[i][3]).append("\"");
        } else {
          sb.append(fieldValues[i][3]);
        }
        first = false;
      }
      // upper bound
      if (hasUpperBound) {
        if (!first) sb.append(",");
        boolean isStringField = (i == 1);
        sb.append("\"").append(fieldIds[i] + 6).append("\":");
        if (isStringField) {
          sb.append("\"").append(fieldValues[i][4]).append("\"");
        } else {
          sb.append(fieldValues[i][4]);
        }
      }
      sb.append("}");
      if (i < fieldIds.length - 1) {
        sb.append(",");
      }
    }
    sb.append("}}");
    return sb.toString();
  }

  private static DataFile dataFileWithContentStats(
      PartitionSpec spec,
      boolean hasValueCount,
      boolean hasNullValueCount,
      boolean hasNanValueCount,
      boolean hasLowBound,
      boolean hasUpperBound) {

    BaseFieldStats<Integer> intFieldStats =
        BaseFieldStats.<Integer>builder()
            .fieldId(1)
            .type(Types.IntegerType.get())
            .valueCount(hasValueCount ? 90L : null)
            .nullValueCount(hasNullValueCount ? 10L : null)
            .nanValueCount(hasNanValueCount ? 0L : null)
            .lowerBound(hasLowBound ? 1000000 : null)
            .upperBound(hasUpperBound ? 5000000 : null)
            .build();

    BaseFieldStats<String> stringFieldStats =
        BaseFieldStats.<String>builder()
            .fieldId(2)
            .type(Types.StringType.get())
            .valueCount(hasValueCount ? 180L : null)
            .nullValueCount(hasNullValueCount ? 20L : null)
            .nanValueCount(hasNanValueCount ? 0L : null)
            .lowerBound(hasLowBound ? "02000000" : null)
            .upperBound(hasUpperBound ? "0A000000" : null)
            .build();

    BaseContentStats contentStats =
        BaseContentStats.builder().withFieldStats(List.of(intFieldStats, stringFieldStats)).build();

    DataFiles.Builder builder =
        DataFiles.builder(spec)
            .withPath("/path/to/data-with-stats.parquet")
            .withContentStats(contentStats)
            .withRecordCount(10L)
            .withFileSizeInBytes(350)
            .withSplitOffsets(Arrays.asList(128L, 256L))
            .withEncryptionKeyMetadata(ByteBuffer.wrap(new byte[16]))
            .withSortOrder(
                SortOrder.builderFor(TestBase.SCHEMA)
                    .withOrderId(1)
                    .sortBy("id", SortDirection.ASC, NullOrder.NULLS_FIRST)
                    .build());

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
            .withEncryptionKeyMetadata(ByteBuffer.wrap(new byte[16]))
            .withSortOrder(
                SortOrder.builderFor(TestBase.SCHEMA)
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
        Arguments.of(TestBase.SPEC, dv(TestBase.SPEC), dvJson()),
        Arguments.of(
            PartitionSpec.unpartitioned(),
            deleteFileWithRequiredOnly(PartitionSpec.unpartitioned()),
            deleteFileJsonWithRequiredOnly(PartitionSpec.unpartitioned())),
        Arguments.of(
            PartitionSpec.unpartitioned(),
            deleteFileWithAllOptional(PartitionSpec.unpartitioned()),
            deleteFileJsonWithAllOptional(PartitionSpec.unpartitioned())),
        Arguments.of(
            TestBase.SPEC,
            deleteFileWithRequiredOnly(TestBase.SPEC),
            deleteFileJsonWithRequiredOnly(TestBase.SPEC)),
        Arguments.of(
            TestBase.SPEC,
            deleteFileWithAllOptional(TestBase.SPEC),
            deleteFileJsonWithAllOptional(TestBase.SPEC)),
        Arguments.of(
            TestBase.SPEC, deleteFileWithDataRef(TestBase.SPEC), deleteFileWithDataRefJson()));
  }

  private static DeleteFile deleteFileWithDataRef(PartitionSpec spec) {
    PartitionData partitionData = new PartitionData(spec.partitionType());
    partitionData.set(0, 4);
    return new GenericDeleteFile(
        spec.specId(),
        FileContent.POSITION_DELETES,
        "/path/to/delete.parquet",
        FileFormat.PARQUET,
        partitionData,
        1234,
        new Metrics(10L, null, null, null, null),
        null,
        null,
        null,
        null,
        null,
        "/path/to/data/file.parquet",
        null,
        null);
  }

  private static String deleteFileWithDataRefJson() {
    return "{\"spec-id\":0,\"content\":\"POSITION_DELETES\",\"file-path\":\"/path/to/delete.parquet\","
        + "\"file-format\":\"PARQUET\",\"partition\":{\"1000\":4},\"file-size-in-bytes\":1234,"
        + "\"record-count\":10,\"referenced-data-file\":\"/path/to/data/file.parquet\"}";
  }

  private static DeleteFile dv(PartitionSpec spec) {
    PartitionData partitionData = new PartitionData(spec.partitionType());
    partitionData.set(0, 4);
    return new GenericDeleteFile(
        spec.specId(),
        FileContent.POSITION_DELETES,
        "/path/to/delete.puffin",
        FileFormat.PUFFIN,
        partitionData,
        1234,
        new Metrics(10L, null, null, null, null),
        null,
        null,
        null,
        null,
        null,
        "/path/to/data/file.parquet",
        4L,
        40L);
  }

  private static String dvJson() {
    return "{\"spec-id\":0,\"content\":\"POSITION_DELETES\",\"file-path\":\"/path/to/delete.puffin\","
        + "\"file-format\":\"PUFFIN\",\"partition\":{\"1000\":4},\"file-size-in-bytes\":1234,\"record-count\":10,"
        + "\"referenced-data-file\":\"/path/to/data/file.parquet\",\"content-offset\":4,\"content-size-in-bytes\":40}";
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
        null,
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
        null,
        new int[] {3},
        1,
        Collections.singletonList(128L),
        ByteBuffer.wrap(new byte[16]),
        null,
        null,
        null);
  }

  private static String deleteFileJsonWithRequiredOnly(PartitionSpec spec) {
    if (spec.isUnpartitioned()) {
      return "{\"spec-id\":0,\"content\":\"POSITION_DELETES\",\"file-path\":\"/path/to/delete-a.parquet\","
          + "\"file-format\":\"PARQUET\",\"partition\":{},\"file-size-in-bytes\":1234,\"record-count\":9}";
    } else {
      return "{\"spec-id\":0,\"content\":\"POSITION_DELETES\",\"file-path\":\"/path/to/delete-a.parquet\","
          + "\"file-format\":\"PARQUET\",\"partition\":{\"1000\":9},\"file-size-in-bytes\":1234,\"record-count\":9}";
    }
  }

  private static String deleteFileJsonWithAllOptional(PartitionSpec spec) {
    if (spec.isUnpartitioned()) {
      return "{\"spec-id\":0,\"content\":\"EQUALITY_DELETES\",\"file-path\":\"/path/to/delete-with-stats.parquet\","
          + "\"file-format\":\"PARQUET\",\"partition\":{},\"file-size-in-bytes\":1234,\"record-count\":10,"
          + "\"column-sizes\":{\"keys\":[3,4],\"values\":[100,200]},"
          + "\"value-counts\":{\"keys\":[3,4],\"values\":[90,180]},"
          + "\"null-value-counts\":{\"keys\":[3,4],\"values\":[10,20]},"
          + "\"nan-value-counts\":{\"keys\":[3,4],\"values\":[0,0]},"
          + "\"lower-bounds\":{\"keys\":[3,4],\"values\":[\"01000000\",\"02000000\"]},"
          + "\"upper-bounds\":{\"keys\":[3,4],\"values\":[\"05000000\",\"0A000000\"]},"
          + "\"key-metadata\":\"00000000000000000000000000000000\","
          + "\"split-offsets\":[128],\"equality-ids\":[3],\"sort-order-id\":1}";
    } else {
      return "{\"spec-id\":0,\"content\":\"EQUALITY_DELETES\",\"file-path\":\"/path/to/delete-with-stats.parquet\","
          + "\"file-format\":\"PARQUET\",\"partition\":{\"1000\":9},\"file-size-in-bytes\":1234,\"record-count\":10,"
          + "\"column-sizes\":{\"keys\":[3,4],\"values\":[100,200]},"
          + "\"value-counts\":{\"keys\":[3,4],\"values\":[90,180]},"
          + "\"null-value-counts\":{\"keys\":[3,4],\"values\":[10,20]},"
          + "\"nan-value-counts\":{\"keys\":[3,4],\"values\":[0,0]},"
          + "\"lower-bounds\":{\"keys\":[3,4],\"values\":[\"01000000\",\"02000000\"]},"
          + "\"upper-bounds\":{\"keys\":[3,4],\"values\":[\"05000000\",\"0A000000\"]},"
          + "\"key-metadata\":\"00000000000000000000000000000000\","
          + "\"split-offsets\":[128],\"equality-ids\":[3],\"sort-order-id\":1}";
    }
  }

  static void assertContentFileEquals(
      ContentFile<?> expected, ContentFile<?> actual, PartitionSpec spec) {
    assertThat(actual.getClass()).isEqualTo(expected.getClass());
    assertThat(actual.specId()).isEqualTo(expected.specId());
    assertThat(actual.content()).isEqualTo(expected.content());
    assertThat(actual.location()).isEqualTo(expected.location());
    assertThat(actual.format()).isEqualTo(expected.format());
    assertThat(actual.partition())
        .usingComparator(Comparators.forType(spec.partitionType()))
        .isEqualTo(expected.partition());
    assertThat(actual.recordCount()).isEqualTo(expected.recordCount());
    assertThat(actual.fileSizeInBytes()).isEqualTo(expected.fileSizeInBytes());
    assertThat(actual.columnSizes()).isEqualTo(expected.columnSizes());
    assertThat(actual.valueCounts()).isEqualTo(expected.valueCounts());
    assertThat(actual.nullValueCounts()).isEqualTo(expected.nullValueCounts());
    assertThat(actual.nanValueCounts()).isEqualTo(expected.nanValueCounts());
    assertThat(actual.lowerBounds()).isEqualTo(expected.lowerBounds());
    assertThat(actual.upperBounds()).isEqualTo(expected.upperBounds());
    assertThat(actual.keyMetadata()).isEqualTo(expected.keyMetadata());
    assertThat(actual.splitOffsets()).isEqualTo(expected.splitOffsets());
    assertThat(actual.equalityFieldIds()).isEqualTo(expected.equalityFieldIds());
    assertThat(actual.sortOrderId()).isEqualTo(expected.sortOrderId());
    assertThat(actual.contentStats()).isEqualTo(expected.contentStats());
  }
}
