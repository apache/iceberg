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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.JsonUtil;
import org.junit.jupiter.api.Test;

public class TestDataTaskParser {
  // copied from SnapshotsTable to avoid making it package public
  private static final Schema SNAPSHOT_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "committed_at", Types.TimestampType.withZone()),
          Types.NestedField.required(2, "snapshot_id", Types.LongType.get()),
          Types.NestedField.optional(3, "parent_id", Types.LongType.get()),
          Types.NestedField.optional(4, "operation", Types.StringType.get()),
          Types.NestedField.optional(5, "manifest_list", Types.StringType.get()),
          Types.NestedField.optional(
              6,
              "summary",
              Types.MapType.ofRequired(7, 8, Types.StringType.get(), Types.StringType.get())));

  // copied from SnapshotsTable to avoid making it package public
  private static StaticDataTask.Row snapshotToRow(Snapshot snap) {
    return StaticDataTask.Row.of(
        snap.timestampMillis() * 1000,
        snap.snapshotId(),
        snap.parentId(),
        snap.operation(),
        snap.manifestListLocation(),
        snap.summary());
  }

  @Test
  public void nullCheck() throws Exception {
    StringWriter writer = new StringWriter();
    JsonGenerator generator = JsonUtil.factory().createGenerator(writer);

    assertThatThrownBy(() -> DataTaskParser.toJson(null, generator))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid data task: null");

    assertThatThrownBy(() -> DataTaskParser.toJson((StaticDataTask) createDataTask(), null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid JSON generator: null");

    assertThatThrownBy(() -> DataTaskParser.fromJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid JSON node for data task: null");
  }

  @Test
  public void invalidJsonNode() throws Exception {
    String jsonStr = "{\"str\":\"1\", \"arr\":[]}";
    ObjectMapper mapper = new ObjectMapper();
    JsonNode rootNode = mapper.reader().readTree(jsonStr);

    assertThatThrownBy(() -> DataTaskParser.fromJson(rootNode.get("str")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid JSON node for data task: non-object ");

    assertThatThrownBy(() -> DataTaskParser.fromJson(rootNode.get("arr")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid JSON node for data task: non-object ");
  }

  @Test
  public void missingFields() throws Exception {
    ObjectMapper mapper = new ObjectMapper();

    String missingSchemaStr = "{}";
    JsonNode missingSchemaNode = mapper.reader().readTree(missingSchemaStr);
    assertThatThrownBy(() -> DataTaskParser.fromJson(missingSchemaNode))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot parse missing field: schema");

    String missingProjectionStr =
        "{"
            + "\"schema\":{\"type\":\"struct\",\"schema-id\":0,"
            + "\"fields\":[{\"id\":1,\"name\":\"committed_at\",\"required\":true,\"type\":\"timestamptz\"}]}"
            + "}";
    JsonNode missingProjectionNode = mapper.reader().readTree(missingProjectionStr);
    assertThatThrownBy(() -> DataTaskParser.fromJson(missingProjectionNode))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot parse missing field: projection");

    String missingMetadataFileStr =
        "{"
            + "\"schema\":{\"type\":\"struct\",\"schema-id\":0,"
            + "\"fields\":[{\"id\":1,\"name\":\"committed_at\",\"required\":true,\"type\":\"timestamptz\"}]},"
            + "\"projection\":{\"type\":\"struct\",\"schema-id\":0,"
            + "\"fields\":[{\"id\":1,\"name\":\"committed_at\",\"required\":true,\"type\":\"timestamptz\"}]}"
            + "}";
    JsonNode missingMetadataFileNode = mapper.reader().readTree(missingMetadataFileStr);
    assertThatThrownBy(() -> DataTaskParser.fromJson(missingMetadataFileNode))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot parse missing field: metadata-file");

    String missingTableRowsStr =
        "{\"task-type\":\"data-task\","
            + "\"schema\":{\"type\":\"struct\",\"schema-id\":0,"
            + "\"fields\":[{\"id\":1,\"name\":\"committed_at\",\"required\":true,\"type\":\"timestamptz\"},"
            + "{\"id\":2,\"name\":\"snapshot_id\",\"required\":true,\"type\":\"long\"},"
            + "{\"id\":3,\"name\":\"parent_id\",\"required\":false,\"type\":\"long\"},"
            + "{\"id\":4,\"name\":\"operation\",\"required\":false,\"type\":\"string\"},"
            + "{\"id\":5,\"name\":\"manifest_list\",\"required\":false,\"type\":\"string\"},"
            + "{\"id\":6,\"name\":\"summary\",\"required\":false,\"type\":{\"type\":\"map\","
            + "\"key-id\":7,\"key\":\"string\",\"value-id\":8,"
            + "\"value\":\"string\",\"value-required\":true}}]},"
            + "\"projection\":{\"type\":\"struct\",\"schema-id\":0,"
            + "\"fields\":[{\"id\":1,\"name\":\"committed_at\",\"required\":true,\"type\":\"timestamptz\"},"
            + "{\"id\":2,\"name\":\"snapshot_id\",\"required\":true,\"type\":\"long\"},"
            + "{\"id\":3,\"name\":\"parent_id\",\"required\":false,\"type\":\"long\"},"
            + "{\"id\":4,\"name\":\"operation\",\"required\":false,\"type\":\"string\"},"
            + "{\"id\":5,\"name\":\"manifest_list\",\"required\":false,\"type\":\"string\"},"
            + "{\"id\":6,\"name\":\"summary\",\"required\":false,\"type\":{\"type\":\"map\","
            + "\"key-id\":7,\"key\":\"string\",\"value-id\":8,"
            + "\"value\":\"string\",\"value-required\":true}}]},"
            + "\"metadata-file\":{\"spec-id\":0,\"content\":\"data\","
            + "\"file-path\":\"/tmp/metadata2.json\","
            + "\"file-format\":\"metadata\",\"partition\":[],"
            + "\"file-size-in-bytes\":0,\"record-count\":2,\"sort-order-id\":0}"
            + "}";
    JsonNode missingTableRowsNode = mapper.reader().readTree(missingTableRowsStr);
    assertThatThrownBy(() -> DataTaskParser.fromJson(missingTableRowsNode))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot parse missing field: rows");
  }

  @Test
  public void roundTripSerde() {
    StaticDataTask dataTask = (StaticDataTask) createDataTask();
    String jsonStr = ScanTaskParser.toJson(dataTask);
    assertThat(jsonStr).isEqualTo(snapshotsDataTaskJson());
    StaticDataTask deserializedTask = (StaticDataTask) ScanTaskParser.fromJson(jsonStr, true);
    assertDataTaskEquals(dataTask, deserializedTask);
  }

  @Test
  public void testDataTaskParsesFieldIdPartitionMap() {
    String jsonStr =
        "{\"task-type\":\"data-task\","
            + "\"schema\":{\"type\":\"struct\",\"schema-id\":0,"
            + "\"fields\":[{\"id\":1,\"name\":\"committed_at\",\"required\":true,\"type\":\"timestamptz\"}]},"
            + "\"projection\":{\"type\":\"struct\",\"schema-id\":0,"
            + "\"fields\":[{\"id\":1,\"name\":\"committed_at\",\"required\":true,\"type\":\"timestamptz\"}]},"
            + "\"metadata-file\":{\"spec-id\":0,\"content\":\"data\",\"file-path\":\"/tmp/metadata.json\","
            + "\"file-format\":\"metadata\",\"partition\":{},\"file-size-in-bytes\":0,\"record-count\":1,\"sort-order-id\":0},"
            + "\"rows\":[{\"1\":\"2009-02-13T23:31:30+00:00\"}]}";

    StaticDataTask deserializedTask = (StaticDataTask) ScanTaskParser.fromJson(jsonStr, true);

    assertThat(deserializedTask.metadataFile().partition().size()).isEqualTo(0);
  }

  private DataTask createDataTask() {
    Map<String, String> summary1 =
        ImmutableMap.of(
            "added-data-files", "1",
            "added-records", "1",
            "added-files-size", "10",
            "changed-partition-count", "1",
            "total-records", "1",
            "total-files-size", "10",
            "total-data-files", "1",
            "total-delete-files", "0",
            "total-position-deletes", "0",
            "total-equality-deletes", "0");

    Map<String, String> summary2 =
        ImmutableMap.of(
            "added-data-files", "1",
            "added-records", "1",
            "added-files-size", "10",
            "changed-partition-count", "1",
            "total-records", "2",
            "total-files-size", "20",
            "total-data-files", "2",
            "total-delete-files", "0",
            "total-position-deletes", "0",
            "total-equality-deletes", "0");

    List<Snapshot> snapshots =
        Arrays.asList(
            new BaseSnapshot(
                1L,
                1L,
                null,
                1234567890000L,
                "append",
                summary1,
                1,
                "file:/tmp/manifest1.avro",
                null,
                null,
                null),
            new BaseSnapshot(
                2L,
                2L,
                1L,
                9876543210000L,
                "append",
                summary2,
                1,
                "file:/tmp/manifest2.avro",
                null,
                null,
                null));

    return StaticDataTask.of(
        Files.localInput("file:/tmp/metadata2.json"),
        SNAPSHOT_SCHEMA,
        SNAPSHOT_SCHEMA,
        snapshots,
        TestDataTaskParser::snapshotToRow);
  }

  private String snapshotsDataTaskJson() {
    return "{\"task-type\":\"data-task\","
        + "\"schema\":{\"type\":\"struct\",\"schema-id\":0,"
        + "\"fields\":[{\"id\":1,\"name\":\"committed_at\",\"required\":true,\"type\":\"timestamptz\"},"
        + "{\"id\":2,\"name\":\"snapshot_id\",\"required\":true,\"type\":\"long\"},"
        + "{\"id\":3,\"name\":\"parent_id\",\"required\":false,\"type\":\"long\"},"
        + "{\"id\":4,\"name\":\"operation\",\"required\":false,\"type\":\"string\"},"
        + "{\"id\":5,\"name\":\"manifest_list\",\"required\":false,\"type\":\"string\"},"
        + "{\"id\":6,\"name\":\"summary\",\"required\":false,\"type\":{\"type\":\"map\","
        + "\"key-id\":7,\"key\":\"string\",\"value-id\":8,"
        + "\"value\":\"string\",\"value-required\":true}}]},"
        + "\"projection\":{\"type\":\"struct\",\"schema-id\":0,"
        + "\"fields\":[{\"id\":1,\"name\":\"committed_at\",\"required\":true,\"type\":\"timestamptz\"},"
        + "{\"id\":2,\"name\":\"snapshot_id\",\"required\":true,\"type\":\"long\"},"
        + "{\"id\":3,\"name\":\"parent_id\",\"required\":false,\"type\":\"long\"},"
        + "{\"id\":4,\"name\":\"operation\",\"required\":false,\"type\":\"string\"},"
        + "{\"id\":5,\"name\":\"manifest_list\",\"required\":false,\"type\":\"string\"},"
        + "{\"id\":6,\"name\":\"summary\",\"required\":false,\"type\":{\"type\":\"map\","
        + "\"key-id\":7,\"key\":\"string\",\"value-id\":8,"
        + "\"value\":\"string\",\"value-required\":true}}]},"
        + "\"metadata-file\":{\"spec-id\":0,\"content\":\"data\","
        + "\"file-path\":\"/tmp/metadata2.json\","
        + "\"file-format\":\"metadata\",\"partition\":[],"
        + "\"file-size-in-bytes\":0,\"record-count\":2,\"sort-order-id\":0},"
        + "\"rows\":[{\"1\":\"2009-02-13T23:31:30+00:00\",\"2\":1,\"4\":\"append\","
        + "\"5\":\"file:/tmp/manifest1.avro\","
        + "\"6\":{\"keys\":[\"added-data-files\",\"added-records\",\"added-files-size\",\"changed-partition-count\","
        + "\"total-records\",\"total-files-size\",\"total-data-files\",\"total-delete-files\","
        + "\"total-position-deletes\",\"total-equality-deletes\"],"
        + "\"values\":[\"1\",\"1\",\"10\",\"1\",\"1\",\"10\",\"1\",\"0\",\"0\",\"0\"]}},"
        + "{\"1\":\"2282-12-22T20:13:30+00:00\",\"2\":2,\"3\":1,\"4\":\"append\","
        + "\"5\":\"file:/tmp/manifest2.avro\","
        + "\"6\":{\"keys\":[\"added-data-files\",\"added-records\",\"added-files-size\",\"changed-partition-count\","
        + "\"total-records\",\"total-files-size\",\"total-data-files\",\"total-delete-files\","
        + "\"total-position-deletes\",\"total-equality-deletes\"],"
        + "\"values\":[\"1\",\"1\",\"10\",\"1\",\"2\",\"20\",\"2\",\"0\",\"0\",\"0\"]}}]}";
  }

  private void assertDataTaskEquals(StaticDataTask expected, StaticDataTask actual) {
    assertThat(actual.schema().asStruct())
        .as("Schema should match")
        .isEqualTo(expected.schema().asStruct());

    assertThat(actual.projectedSchema().asStruct())
        .as("Projected schema should match")
        .isEqualTo(expected.projectedSchema().asStruct());

    TestContentFileParser.assertContentFileEquals(
        expected.metadataFile(), actual.metadataFile(), PartitionSpec.unpartitioned());

    List<StructLike> expectedRows = Lists.newArrayList(expected.rows());
    List<StructLike> actualRows = Lists.newArrayList(actual.rows());
    assertThat(actualRows).hasSameSizeAs(expectedRows);

    // all fields are primitive types or map
    Schema schema = expected.schema();
    for (int i = 0; i < expectedRows.size(); ++i) {
      StructLike expectedRow = expectedRows.get(i);
      StructLike actualRow = actualRows.get(i);
      for (int pos = 0; pos < expectedRow.size(); ++pos) {
        Class<?> javaClass = schema.columns().get(pos).type().typeId().javaClass();
        assertThat(actualRow.get(pos, javaClass)).isEqualTo(expectedRow.get(pos, javaClass));
      }
    }
  }
}
