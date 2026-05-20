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
import java.util.Map;
import java.util.stream.Stream;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
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

  @Test
  public void testNanCountsOnlyWritesNanValueCounts() throws Exception {
    PartitionSpec spec = PartitionSpec.unpartitioned();
    DataFile dataFile = dataFileWithOnlyNanCounts(spec);
    String jsonStr = ContentFileParser.toJson(dataFile, spec);
    // ensure nan counts are present and null counts are not emitted
    assertThat(jsonStr).contains("\"nan-value-counts\"");
    assertThat(jsonStr).doesNotContain("\"null-value-counts\"");
    JsonNode jsonNode = JsonUtil.mapper().readTree(jsonStr);
    ContentFile<?> deserialized =
        ContentFileParser.fromJson(jsonNode, Map.of(TestBase.SPEC.specId(), spec));
    assertThat(deserialized).isInstanceOf(DataFile.class);
    assertContentFileEquals(dataFile, deserialized, spec);
  }

  @ParameterizedTest
  @MethodSource("provideSpecAndDataFile")
  public void testDataFile(PartitionSpec spec, DataFile dataFile, String expectedJson)
      throws Exception {
    String jsonStr = ContentFileParser.toJson(dataFile, spec);
    assertThat(jsonStr).isEqualTo(expectedJson);
    JsonNode jsonNode = JsonUtil.mapper().readTree(jsonStr);
    ContentFile<?> deserializedContentFile =
        ContentFileParser.fromJson(jsonNode, Map.of(spec.specId(), spec));
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
        ContentFileParser.fromJson(jsonNode, Map.of(spec.specId(), spec));
    assertThat(deserializedContentFile).isInstanceOf(DeleteFile.class);
    assertContentFileEquals(deleteFile, deserializedContentFile, spec);
  }

  @Test
  public void testPartitionJsonArrayWrongSize() throws Exception {
    PartitionSpec spec = PartitionSpec.builderFor(TestBase.SCHEMA).identity("data").build();
    String jsonStr =
        "{\"spec-id\":0,\"content\":\"data\",\"file-path\":\"/path/to/data.parquet\","
            + "\"file-format\":\"parquet\",\"partition\":[],\"file-size-in-bytes\":10,"
            + "\"record-count\":1}";

    JsonNode jsonNode = JsonUtil.mapper().readTree(jsonStr);

    assertThatThrownBy(() -> ContentFileParser.fromJson(jsonNode, Map.of(spec.specId(), spec)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid partition data size");
  }

  @Test
  public void testPartitionJsonInvalidType() throws Exception {
    PartitionSpec spec = PartitionSpec.builderFor(TestBase.SCHEMA).identity("data").build();
    String jsonStr =
        "{\"spec-id\":0,\"content\":\"data\",\"file-path\":\"/path/to/data.parquet\","
            + "\"file-format\":\"parquet\",\"partition\":\"invalid\",\"file-size-in-bytes\":10,"
            + "\"record-count\":1}";

    JsonNode jsonNode = JsonUtil.mapper().readTree(jsonStr);

    assertThatThrownBy(() -> ContentFileParser.fromJson(jsonNode, Map.of(spec.specId(), spec)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("expected array or object");
  }

  @Test
  public void testParsesFieldIdPartitionMap() throws Exception {
    PartitionSpec spec = PartitionSpec.builderFor(TestBase.SCHEMA).identity("data").build();
    String legacyJson =
        "{\"spec-id\":0,\"content\":\"data\",\"file-path\":\"/path/to/data.parquet\","
            + "\"file-format\":\"parquet\",\"partition\":{\"1000\":\"foo\"},\"file-size-in-bytes\":10,"
            + "\"record-count\":1}";

    JsonNode jsonNode = JsonUtil.mapper().readTree(legacyJson);
    ContentFile<?> deserializedContentFile =
        ContentFileParser.fromJson(jsonNode, Map.of(spec.specId(), spec));

    assertThat(deserializedContentFile).isInstanceOf(DataFile.class);
    assertThat(deserializedContentFile.partition().get(0, String.class)).isEqualTo("foo");
  }

  @Test
  public void testPartitionStructObjectContainsExtraField() throws Exception {
    PartitionSpec spec = PartitionSpec.builderFor(TestBase.SCHEMA).identity("data").build();
    String jsonStr =
        "{\"spec-id\":0,\"content\":\"data\",\"file-path\":\"/path/to/data.parquet\","
            + "\"file-format\":\"parquet\",\"partition\":{\"1000\":\"foo\",\"9999\":\"bar\"},"
            + "\"file-size-in-bytes\":10,\"record-count\":1}";

    JsonNode jsonNode = JsonUtil.mapper().readTree(jsonStr);
    assertThatThrownBy(() -> ContentFileParser.fromJson(jsonNode, Map.of(spec.specId(), spec)))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Invalid partition data size");
  }

  @Test
  public void testPartitionStructObjectEmptyIsNull() throws Exception {
    PartitionSpec spec = PartitionSpec.builderFor(TestBase.SCHEMA).identity("data").build();
    String jsonStr =
        "{\"spec-id\":0,\"content\":\"data\",\"file-path\":\"/path/to/data.parquet\","
            + "\"file-format\":\"parquet\",\"partition\":{},\"file-size-in-bytes\":10,"
            + "\"record-count\":1}";

    JsonNode jsonNode = JsonUtil.mapper().readTree(jsonStr);
    ContentFile<?> contentFile = ContentFileParser.fromJson(jsonNode, Map.of(spec.specId(), spec));
    assertThat(contentFile).isInstanceOf(DataFile.class);
    assertThat(contentFile.partition().get(0, String.class)).isNull();
  }

  @Test
  public void testPartitionArrayRespectsSpecOrder() throws Exception {
    PartitionSpec spec =
        PartitionSpec.builderFor(TestBase.SCHEMA).identity("id").identity("data").build();

    PartitionData partitionData = new PartitionData(spec.partitionType());
    partitionData.set(0, 4);
    partitionData.set(1, "foo");

    DataFile dataFile =
        DataFiles.builder(spec)
            .withPath("/path/to/data.parquet")
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .withPartition(partitionData)
            .build();

    String jsonStr = ContentFileParser.toJson(dataFile, spec);

    // Verify partition values are serialized as array in correct order
    assertThat(jsonStr).contains("\"partition\":[4,\"foo\"]");

    JsonNode jsonNode = JsonUtil.mapper().readTree(jsonStr);
    ContentFile<?> deserializedContentFile =
        ContentFileParser.fromJson(jsonNode, Map.of(spec.specId(), spec));

    assertThat(deserializedContentFile).isInstanceOf(DataFile.class);
    assertThat(deserializedContentFile.partition().get(0, Integer.class)).isEqualTo(4);
    assertThat(deserializedContentFile.partition().get(1, String.class)).isEqualTo("foo");
  }

  @Test
  public void testInvalidContentType() throws Exception {
    String jsonStr =
        "{\"spec-id\":0,"
            + "\"content\":\"invalid-content\","
            + "\"file-path\":\"/path/to/file.parquet\","
            + "\"file-format\":\"parquet\","
            + "\"partition\":{},"
            + "\"file-size-in-bytes\":1,"
            + "\"record-count\":1}";

    JsonNode node = JsonUtil.mapper().readTree(jsonStr);

    assertThatThrownBy(
            () -> ContentFileParser.fromJson(node, Map.of(0, PartitionSpec.unpartitioned())))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid file content value: 'invalid-content'");
  }

  @Test
  public void testUppercaseFileFormat() throws Exception {
    String jsonStr =
        "{\"spec-id\":0,"
            + "\"content\":\"data\","
            + "\"file-path\":\"/path/to/file.parquet\","
            + "\"file-format\":\"PARQUET\","
            + "\"partition\":{},"
            + "\"file-size-in-bytes\":1,"
            + "\"record-count\":1}";

    JsonNode jsonNode = JsonUtil.mapper().readTree(jsonStr);
    ContentFile<?> deserializedContentFile =
        ContentFileParser.fromJson(jsonNode, Map.of(0, PartitionSpec.unpartitioned()));
    assertThat(deserializedContentFile.format()).isEqualTo(FileFormat.PARQUET);
  }

  @ParameterizedTest
  @MethodSource("enumContentTypeCases")
  public void testEnumContentTypeSerialization(FileContent content, String expectedJsonContent)
      throws Exception {
    String jsonStr =
        "{\"spec-id\":0,"
            + "\"content\":\""
            + content.name()
            + "\","
            + "\"file-path\":\"/path/to/data.parquet\","
            + "\"file-format\":\"parquet\","
            + "\"partition\":{},"
            + "\"file-size-in-bytes\":1,"
            + "\"record-count\":1}";

    JsonNode jsonNode = JsonUtil.mapper().readTree(jsonStr);
    ContentFile<?> deserializedContentFile =
        ContentFileParser.fromJson(jsonNode, Map.of(0, PartitionSpec.unpartitioned()));
    assertThat(deserializedContentFile.content()).isEqualTo(content);

    String serializedStr =
        ContentFileParser.toJson(deserializedContentFile, PartitionSpec.unpartitioned());
    assertThat(serializedStr).contains("\"content\":\"" + expectedJsonContent + "\"");
  }

  private static Stream<Arguments> enumContentTypeCases() {
    return Stream.of(
        Arguments.of(FileContent.DATA, "data"),
        Arguments.of(FileContent.POSITION_DELETES, "position-deletes"),
        Arguments.of(FileContent.EQUALITY_DELETES, "equality-deletes"));
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

  private static DataFile dataFileWithOnlyNanCounts(PartitionSpec spec) {
    DataFiles.Builder builder =
        DataFiles.builder(spec)
            .withPath("/path/to/data-nan-only.parquet")
            .withMetrics(
                new Metrics(
                    1L, // record count
                    null, // column sizes
                    null, // value counts
                    null, // null value counts (intentionally null)
                    ImmutableMap.of(3, 0L), // nan value counts present
                    null, // lower bounds
                    null // upper bounds
                    ))
            .withFileSizeInBytes(10)
            .withRecordCount(1);

    if (spec.isPartitioned()) {
      builder.withPartitionPath("data_bucket=1");
    }

    return builder.build();
  }

  private static String dataFileJsonWithRequiredOnly(PartitionSpec spec) {
    if (spec.isUnpartitioned()) {
      return "{\"spec-id\":0,\"content\":\"data\",\"file-path\":\"/path/to/data-a.parquet\",\"file-format\":\"parquet\","
          + "\"partition\":[],\"file-size-in-bytes\":10,\"record-count\":1,\"sort-order-id\":0}";
    } else {
      return "{\"spec-id\":0,\"content\":\"data\",\"file-path\":\"/path/to/data-a.parquet\",\"file-format\":\"parquet\","
          + "\"partition\":[1],\"file-size-in-bytes\":10,\"record-count\":1,\"sort-order-id\":0}";
    }
  }

  private static String dataFileJsonWithAllOptional(PartitionSpec spec) {
    if (spec.isUnpartitioned()) {
      return "{\"spec-id\":0,\"content\":\"data\",\"file-path\":\"/path/to/data-with-stats.parquet\","
          + "\"file-format\":\"parquet\",\"partition\":[],\"file-size-in-bytes\":350,\"record-count\":10,"
          + "\"column-sizes\":{\"keys\":[3,4],\"values\":[100,200]},"
          + "\"value-counts\":{\"keys\":[3,4],\"values\":[90,180]},"
          + "\"null-value-counts\":{\"keys\":[3,4],\"values\":[10,20]},"
          + "\"nan-value-counts\":{\"keys\":[3,4],\"values\":[0,0]},"
          + "\"lower-bounds\":{\"keys\":[3,4],\"values\":[\"01000000\",\"02000000\"]},"
          + "\"upper-bounds\":{\"keys\":[3,4],\"values\":[\"05000000\",\"0A000000\"]},"
          + "\"key-metadata\":\"00000000000000000000000000000000\","
          + "\"split-offsets\":[128,256],\"sort-order-id\":1}";
    } else {
      return "{\"spec-id\":0,\"content\":\"data\",\"file-path\":\"/path/to/data-with-stats.parquet\","
          + "\"file-format\":\"parquet\",\"partition\":[1],\"file-size-in-bytes\":350,\"record-count\":10,"
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
        "/path/to/data/file.parquet",
        null,
        null);
  }

  private static String deleteFileWithDataRefJson() {
    return "{\"spec-id\":0,\"content\":\"position-deletes\",\"file-path\":\"/path/to/delete.parquet\","
        + "\"file-format\":\"parquet\",\"partition\":[4],\"file-size-in-bytes\":1234,"
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
        "/path/to/data/file.parquet",
        4L,
        40L);
  }

  private static String dvJson() {
    return "{\"spec-id\":0,\"content\":\"position-deletes\",\"file-path\":\"/path/to/delete.puffin\","
        + "\"file-format\":\"puffin\",\"partition\":[4],\"file-size-in-bytes\":1234,\"record-count\":10,"
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
        Collections.singletonList(128L),
        ByteBuffer.wrap(new byte[16]),
        null,
        null,
        null);
  }

  private static String deleteFileJsonWithRequiredOnly(PartitionSpec spec) {
    if (spec.isUnpartitioned()) {
      return "{\"spec-id\":0,\"content\":\"position-deletes\",\"file-path\":\"/path/to/delete-a.parquet\","
          + "\"file-format\":\"parquet\",\"partition\":[],\"file-size-in-bytes\":1234,\"record-count\":9}";
    } else {
      return "{\"spec-id\":0,\"content\":\"position-deletes\",\"file-path\":\"/path/to/delete-a.parquet\","
          + "\"file-format\":\"parquet\",\"partition\":[9],\"file-size-in-bytes\":1234,\"record-count\":9}";
    }
  }

  private static String deleteFileJsonWithAllOptional(PartitionSpec spec) {
    if (spec.isUnpartitioned()) {
      return "{\"spec-id\":0,\"content\":\"equality-deletes\",\"file-path\":\"/path/to/delete-with-stats.parquet\","
          + "\"file-format\":\"parquet\",\"partition\":[],\"file-size-in-bytes\":1234,\"record-count\":10,"
          + "\"column-sizes\":{\"keys\":[3,4],\"values\":[100,200]},"
          + "\"value-counts\":{\"keys\":[3,4],\"values\":[90,180]},"
          + "\"null-value-counts\":{\"keys\":[3,4],\"values\":[10,20]},"
          + "\"nan-value-counts\":{\"keys\":[3,4],\"values\":[0,0]},"
          + "\"lower-bounds\":{\"keys\":[3,4],\"values\":[\"01000000\",\"02000000\"]},"
          + "\"upper-bounds\":{\"keys\":[3,4],\"values\":[\"05000000\",\"0A000000\"]},"
          + "\"key-metadata\":\"00000000000000000000000000000000\","
          + "\"split-offsets\":[128],\"equality-ids\":[3],\"sort-order-id\":1}";
    } else {
      return "{\"spec-id\":0,\"content\":\"equality-deletes\",\"file-path\":\"/path/to/delete-with-stats.parquet\","
          + "\"file-format\":\"parquet\",\"partition\":[9],\"file-size-in-bytes\":1234,\"record-count\":10,"
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
  }
}
