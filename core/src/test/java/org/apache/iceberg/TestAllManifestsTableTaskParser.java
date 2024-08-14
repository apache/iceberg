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
import java.util.Map;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.JsonUtil;
import org.apache.iceberg.util.PartitionUtil;
import org.junit.jupiter.api.Test;

public class TestAllManifestsTableTaskParser {
  @Test
  public void nullCheck() throws Exception {
    StringWriter writer = new StringWriter();
    JsonGenerator generator = JsonUtil.factory().createGenerator(writer);

    assertThatThrownBy(() -> AllManifestsTableTaskParser.toJson(null, generator))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid manifest task: null");

    assertThatThrownBy(() -> AllManifestsTableTaskParser.toJson(createTask(), null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid JSON generator: null");

    assertThatThrownBy(() -> AllManifestsTableTaskParser.fromJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid JSON node for manifest task: null");
  }

  @Test
  public void invalidJsonNode() throws Exception {
    String jsonStr = "{\"str\":\"1\", \"arr\":[]}";
    ObjectMapper mapper = new ObjectMapper();
    JsonNode rootNode = mapper.reader().readTree(jsonStr);

    assertThatThrownBy(() -> AllManifestsTableTaskParser.fromJson(rootNode.get("str")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid JSON node for manifest task: non-object ");

    assertThatThrownBy(() -> AllManifestsTableTaskParser.fromJson(rootNode.get("arr")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid JSON node for manifest task: non-object ");
  }

  @Test
  public void testParser() {
    AllManifestsTable.ManifestListReadTask task = createTask();
    String jsonStr = ScanTaskParser.toJson(task);
    assertThat(jsonStr).isEqualTo(taskJson());
    AllManifestsTable.ManifestListReadTask deserializedTask =
        (AllManifestsTable.ManifestListReadTask) ScanTaskParser.fromJson(jsonStr, false);
    assertTaskEquals(task, deserializedTask);
  }

  static AllManifestsTable.ManifestListReadTask createTask() {
    Schema dataTableSchema = TestBase.SCHEMA;
    HadoopFileIO fileIO = new HadoopFileIO();
    fileIO.initialize(ImmutableMap.of("k1", "v1", "k2", "v2"));
    Map<Integer, PartitionSpec> specsById =
        PartitionUtil.indexSpecs(
            Arrays.asList(PartitionSpec.builderFor(dataTableSchema).bucket("data", 16).build()));

    return new AllManifestsTable.ManifestListReadTask(
        dataTableSchema,
        fileIO,
        AllManifestsTable.MANIFEST_FILE_SCHEMA,
        specsById,
        "/path/manifest-list-file.avro",
        Expressions.equal("id", 1),
        1L);
  }

  private String taskJson() {
    return "{\"task-type\":\"all-manifests-table-task\","
        + "\"data-table-schema\":{\"type\":\"struct\",\"schema-id\":0,"
        + "\"fields\":[{\"id\":3,\"name\":\"id\",\"required\":true,\"type\":\"int\"},"
        + "{\"id\":4,\"name\":\"data\",\"required\":true,\"type\":\"string\"}]},"
        + "\"file-io\":{\"io-impl\":\"org.apache.iceberg.hadoop.HadoopFileIO\","
        + "\"properties\":{\"k1\":\"v1\",\"k2\":\"v2\"}},"
        + "\"schema\":{\"type\":\"struct\",\"schema-id\":0,\"fields\":[{"
        + "\"id\":14,\"name\":\"content\",\"required\":true,\"type\":\"int\"},"
        + "{\"id\":1,\"name\":\"path\",\"required\":true,\"type\":\"string\"},"
        + "{\"id\":2,\"name\":\"length\",\"required\":true,\"type\":\"long\"},"
        + "{\"id\":3,\"name\":\"partition_spec_id\",\"required\":false,\"type\":\"int\"},"
        + "{\"id\":4,\"name\":\"added_snapshot_id\",\"required\":false,\"type\":\"long\"},"
        + "{\"id\":5,\"name\":\"added_data_files_count\",\"required\":false,\"type\":\"int\"},"
        + "{\"id\":6,\"name\":\"existing_data_files_count\",\"required\":false,\"type\":\"int\"},"
        + "{\"id\":7,\"name\":\"deleted_data_files_count\",\"required\":false,\"type\":\"int\"},"
        + "{\"id\":15,\"name\":\"added_delete_files_count\",\"required\":true,\"type\":\"int\"},"
        + "{\"id\":16,\"name\":\"existing_delete_files_count\",\"required\":true,\"type\":\"int\"},"
        + "{\"id\":17,\"name\":\"deleted_delete_files_count\",\"required\":true,\"type\":\"int\"},"
        + "{\"id\":8,\"name\":\"partition_summaries\",\"required\":false,\"type\":"
        + "{\"type\":\"list\",\"element-id\":9,\"element\":{\"type\":\"struct\",\"fields\":[{"
        + "\"id\":10,\"name\":\"contains_null\",\"required\":true,\"type\":\"boolean\"},"
        + "{\"id\":11,\"name\":\"contains_nan\",\"required\":true,\"type\":\"boolean\"},"
        + "{\"id\":12,\"name\":\"lower_bound\",\"required\":false,\"type\":\"string\"},"
        + "{\"id\":13,\"name\":\"upper_bound\",\"required\":false,\"type\":\"string\"}]},\"element-required\":true}},"
        + "{\"id\":18,\"name\":\"reference_snapshot_id\",\"required\":true,\"type\":\"long\"}]},"
        + "\"partition-specs\":[{\"spec-id\":0,\"fields\":[{\"name\":\"data_bucket\","
        + "\"transform\":\"bucket[16]\",\"source-id\":4,\"field-id\":1000}]}],"
        + "\"manifest-list-Location\":\"/path/manifest-list-file.avro\","
        + "\"residual-filter\":{\"type\":\"eq\",\"term\":\"id\",\"value\":1},"
        + "\"reference-snapshot-id\":1}";
  }

  private void assertTaskEquals(
      AllManifestsTable.ManifestListReadTask expected,
      AllManifestsTable.ManifestListReadTask actual) {

    HadoopFileIO expectedIO = (HadoopFileIO) expected.io();
    HadoopFileIO actualIO = (HadoopFileIO) expected.io();
    assertThat(actualIO.properties()).isEqualTo(expectedIO.properties());

    assertThat(expected.schema().sameSchema(actual.schema())).as("Schema should match").isTrue();

    assertThat(actual.specsById()).isEqualTo(expected.specsById());
    assertThat(actual.manifestListLocation()).isEqualTo(expected.manifestListLocation());
    assertThat(actual.residual().toString()).isEqualTo(expected.residual().toString());
    assertThat(actual.referenceSnapshotId()).isEqualTo(expected.referenceSnapshotId());
  }
}
