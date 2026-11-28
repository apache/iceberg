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
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.StorageCredential;
import org.apache.iceberg.io.SupportsStorageCredentials;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.JsonUtil;
import org.apache.iceberg.util.PartitionUtil;
import org.junit.jupiter.api.Test;

public class TestFilesTableTaskParser {
  @Test
  public void nullCheck() throws Exception {
    StringWriter writer = new StringWriter();
    try (JsonGenerator generator = JsonUtil.factory().createGenerator(writer)) {
      assertThatThrownBy(() -> FilesTableTaskParser.toJson(null, generator))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage("Invalid files task: null");

      assertThatThrownBy(() -> FilesTableTaskParser.toJson(createTask(), null))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage("Invalid JSON generator: null");

      assertThatThrownBy(() -> FilesTableTaskParser.fromJson(null))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage("Invalid JSON node for files task: null");
    }
  }

  @Test
  public void invalidJsonNode() throws Exception {
    String jsonStr = "{\"str\":\"1\", \"arr\":[]}";
    ObjectMapper mapper = new ObjectMapper();
    JsonNode rootNode = mapper.reader().readTree(jsonStr);

    assertThatThrownBy(() -> FilesTableTaskParser.fromJson(rootNode.get("str")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid JSON node for files task: non-object ");

    assertThatThrownBy(() -> FilesTableTaskParser.fromJson(rootNode.get("arr")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid JSON node for files task: non-object ");
  }

  @Test
  public void testParser() {
    BaseFilesTable.ManifestReadTask task = createTask();
    String jsonStr = ScanTaskParser.toJson(task);
    assertThat(jsonStr).isEqualTo(taskJson());
    BaseFilesTable.ManifestReadTask deserializedTask =
        (BaseFilesTable.ManifestReadTask) ScanTaskParser.fromJson(jsonStr, false);
    assertTaskEquals(task, deserializedTask);
  }

  @Test
  void parserWithStorageCredentials() {
    TestCatalogUtil.TestFileIOWithStorageCredentials fileIO =
        new TestCatalogUtil.TestFileIOWithStorageCredentials();
    fileIO.initialize(ImmutableMap.of("k1", "v1", "k2", "v2"));
    StorageCredential credential =
        StorageCredential.create(
            "s3://bucket",
            ImmutableMap.of("s3.access-key-id", "key", "s3.secret-access-key", "secret"));
    fileIO.setCredentials(ImmutableList.of(credential));

    Schema schema = TestBase.SCHEMA;
    Map<Integer, PartitionSpec> specsById =
        PartitionUtil.indexSpecs(
            Arrays.asList(PartitionSpec.builderFor(schema).bucket("data", 16).build()));
    ManifestFile manifestFile = TestManifestFileParser.createManifestFile();
    BaseFilesTable.ManifestReadTask task =
        new BaseFilesTable.ManifestReadTask(
            schema, fileIO, specsById, manifestFile, schema, Expressions.equal("id", 1));

    String jsonStr = ScanTaskParser.toJson(task);
    assertThat(jsonStr).contains("\"storage-credentials\":[{\"prefix\":\"s3://bucket\"");

    BaseFilesTable.ManifestReadTask deserializedTask =
        (BaseFilesTable.ManifestReadTask) ScanTaskParser.fromJson(jsonStr, false);
    assertTaskEquals(task, deserializedTask);

    assertThat(deserializedTask.io()).isInstanceOf(SupportsStorageCredentials.class);
    assertThat(((SupportsStorageCredentials) deserializedTask.io()).credentials())
        .containsExactly(credential);
  }

  private BaseFilesTable.ManifestReadTask createTask() {
    Schema schema = TestBase.SCHEMA;
    HadoopFileIO fileIO = new HadoopFileIO();
    fileIO.initialize(ImmutableMap.of("k1", "v1", "k2", "v2"));
    Map<Integer, PartitionSpec> specsById =
        PartitionUtil.indexSpecs(
            Arrays.asList(PartitionSpec.builderFor(schema).bucket("data", 16).build()));
    ManifestFile manifestFile = TestManifestFileParser.createManifestFile();
    return new BaseFilesTable.ManifestReadTask(
        schema, fileIO, specsById, manifestFile, schema, Expressions.equal("id", 1));
  }

  private String taskJson() {
    return "{\"task-type\":\"files-table-task\","
        + "\"schema\":{\"type\":\"struct\",\"schema-id\":0,\"fields\":[{"
        + "\"id\":3,\"name\":\"id\",\"required\":true,\"type\":\"int\"},"
        + "{\"id\":4,\"name\":\"data\",\"required\":true,\"type\":\"string\"}]},"
        + "\"projection\":{\"type\":\"struct\",\"schema-id\":0,\"fields\":[{"
        + "\"id\":3,\"name\":\"id\",\"required\":true,\"type\":\"int\"},"
        + "{\"id\":4,\"name\":\"data\",\"required\":true,\"type\":\"string\"}]},"
        + "\"file-io\":{\"io-impl\":\"org.apache.iceberg.hadoop.HadoopFileIO\","
        + "\"properties\":{\"k1\":\"v1\",\"k2\":\"v2\"}},"
        + "\"partition-specs\":[{\"spec-id\":0,\"fields\":[{"
        + "\"name\":\"data_bucket\",\"transform\":\"bucket[16]\",\"source-id\":4,\"field-id\":1000}]}],"
        + "\"residual-filter\":{\"type\":\"eq\",\"term\":\"id\",\"value\":1},"
        + "\"manifest-file\":{\"path\":\"/path/input.m0.avro\","
        + "\"length\":5878,\"partition-spec-id\":0,\"content\":0,\"sequence-number\":1,\"min-sequence-number\":2,"
        + "\"added-snapshot-id\":12345678901234567,"
        + "\"added-files-count\":1,\"existing-files-count\":3,\"deleted-files-count\":0,"
        + "\"added-rows-count\":10,\"existing-rows-count\":30,\"deleted-rows-count\":0,"
        + "\"partition-field-summary\":[{\"contains-null\":true,\"contains-nan\":false,"
        + "\"lower-bound\":\"0A000000\",\"upper-bound\":\"64000000\"}],\"key-metadata\":\"DB030000\","
        + "\"first-row-id\":10}}";
  }

  private void assertTaskEquals(
      BaseFilesTable.ManifestReadTask expected, BaseFilesTable.ManifestReadTask actual) {
    assertThat(actual.schema().asStruct())
        .as("Schema should match")
        .isEqualTo(expected.schema().asStruct());
    assertThat(actual.projection().asStruct())
        .as("Projected schema should match")
        .isEqualTo(expected.projection().asStruct());

    FileIO expectedIO = expected.io();
    FileIO actualIO = actual.io();
    assertThat(actualIO.properties()).isEqualTo(expectedIO.properties());
    if (expectedIO instanceof SupportsStorageCredentials) {
      assertThat(actualIO).isInstanceOf(SupportsStorageCredentials.class);
      assertThat(((SupportsStorageCredentials) actualIO).credentials())
          .isEqualTo(((SupportsStorageCredentials) expectedIO).credentials());
    }

    assertThat(actual.specsById()).isEqualTo(expected.specsById());
    assertThat(actual.residual().toString()).isEqualTo(expected.residual().toString());
    assertThat(actual.manifest()).isEqualTo(expected.manifest());
  }
}
