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

import java.io.IOException;
import java.nio.file.Path;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestSnapshotJson {
  @TempDir private Path temp;

  public TableOperations ops = new LocalTableOperations(temp);

  @Test
  public void testJsonConversion() throws IOException {
    Snapshot expected =
        MetadataTestUtils.buildTestSnapshotWithExampleValues()
            .setOperation(null)
            .setSummary(null)
            .setSchemaId(1)
            .buildWithExampleManifestList(
                temp,
                ImmutableList.of(
                    MetadataTestUtils.EXAMPLE_MANIFEST_PATH_1,
                    MetadataTestUtils.EXAMPLE_MANIFEST_PATH_2));
    String json = SnapshotParser.toJson(expected);
    Snapshot snapshot = SnapshotParser.fromJson(json);

    assertThat(snapshot.snapshotId()).isEqualTo(expected.snapshotId());
    assertThat(snapshot.allManifests(ops.io())).isEqualTo(expected.allManifests(ops.io()));
    assertThat(snapshot.operation()).isNull();
    assertThat(snapshot.summary()).isNull();
    assertThat(snapshot.schemaId()).isEqualTo(1);
  }

  @Test
  public void testJsonConversionWithoutSchemaId() throws IOException {
    Snapshot expected =
        MetadataTestUtils.buildTestSnapshotWithExampleValues()
            .setOperation(null)
            .setSummary(null)
            .setSchemaId(null)
            .buildWithExampleManifestList(
                temp,
                ImmutableList.of(
                    MetadataTestUtils.EXAMPLE_MANIFEST_PATH_1,
                    MetadataTestUtils.EXAMPLE_MANIFEST_PATH_2));
    String json = SnapshotParser.toJson(expected);
    Snapshot snapshot = SnapshotParser.fromJson(json);

    assertThat(snapshot.snapshotId()).isEqualTo(expected.snapshotId());
    assertThat(snapshot.allManifests(ops.io())).isEqualTo(expected.allManifests(ops.io()));
    assertThat(snapshot.operation()).isNull();
    assertThat(snapshot.summary()).isNull();
    assertThat(snapshot.schemaId()).isNull();
  }

  @Test
  public void testJsonConversionWithOperation() throws IOException {
    Snapshot expected =
        MetadataTestUtils.buildTestSnapshotWithExampleValues()
            .setSequenceNumber(0L)
            .setOperation(DataOperations.REPLACE)
            .buildWithExampleManifestList(
                temp,
                ImmutableList.of(
                    MetadataTestUtils.EXAMPLE_MANIFEST_PATH_1,
                    MetadataTestUtils.EXAMPLE_MANIFEST_PATH_2));

    String json = SnapshotParser.toJson(expected);
    Snapshot snapshot = SnapshotParser.fromJson(json);

    assertThat(snapshot.sequenceNumber())
        .as("Sequence number should default to 0 for v1")
        .isEqualTo(0);
    assertThat(snapshot.snapshotId()).isEqualTo(expected.snapshotId());
    assertThat(snapshot.timestampMillis()).isEqualTo(expected.timestampMillis());
    assertThat(snapshot.parentId()).isEqualTo(expected.parentId());
    assertThat(snapshot.manifestListLocation()).isEqualTo(expected.manifestListLocation());
    assertThat(snapshot.allManifests(ops.io())).isEqualTo(expected.allManifests(ops.io()));
    assertThat(snapshot.operation()).isEqualTo(expected.operation());
    assertThat(snapshot.summary()).isEqualTo(expected.summary());
    assertThat(snapshot.schemaId()).isEqualTo(expected.schemaId());
  }

  @Test
  public void testJsonConversionWithV1Manifests() {
    long parentId = 1;
    long id = 2;

    // this creates a V1 snapshot with manifests
    long timestampMillis = System.currentTimeMillis();
    Snapshot expected =
        MetadataTestUtils.buildTestSnapshot()
            .setSequenceNumber(0)
            .setSnapshotId(id)
            .setParentId(parentId)
            .setTimestampMillis(timestampMillis)
            .setOperation(DataOperations.REPLACE)
            .setSummary(ImmutableMap.of("files-added", "4", "files-deleted", "100"))
            .setSchemaId(3)
            .setV1ManifestLocations(new String[] {"/tmp/manifest1.avro", "/tmp/manifest2.avro"})
            .build();

    String expectedJson =
        String.format(
            "{\n"
                + "  \"snapshot-id\" : 2,\n"
                + "  \"parent-snapshot-id\" : 1,\n"
                + "  \"timestamp-ms\" : %s,\n"
                + "  \"summary\" : {\n"
                + "    \"operation\" : \"replace\",\n"
                + "    \"files-added\" : \"4\",\n"
                + "    \"files-deleted\" : \"100\"\n"
                + "  },\n"
                + "  \"manifests\" : [ \"/tmp/manifest1.avro\", \"/tmp/manifest2.avro\" ],\n"
                + "  \"schema-id\" : 3\n"
                + "}",
            timestampMillis);

    String json = SnapshotParser.toJson(expected, true);
    assertThat(json).isEqualTo(expectedJson);
    Snapshot snapshot = SnapshotParser.fromJson(json);
    assertThat(snapshot).isEqualTo(expected);

    assertThat(snapshot.sequenceNumber())
        .as("Sequence number should default to 0 for v1")
        .isEqualTo(0);
    assertThat(snapshot.snapshotId()).isEqualTo(expected.snapshotId());
    assertThat(snapshot.timestampMillis()).isEqualTo(expected.timestampMillis());
    assertThat(snapshot.parentId()).isEqualTo(expected.parentId());
    assertThat(snapshot.manifestListLocation()).isEqualTo(expected.manifestListLocation());
    assertThat(snapshot.allManifests(ops.io())).isEqualTo(expected.allManifests(ops.io()));
    assertThat(snapshot.operation()).isEqualTo(expected.operation());
    assertThat(snapshot.summary()).isEqualTo(expected.summary());
    assertThat(snapshot.schemaId()).isEqualTo(expected.schemaId());
  }

  @Test
  public void testJsonConversionSummaryWithoutOperation() {
    // This behavior is out of spec, but we don't want to fail on it.
    // Instead, the operation will be set to overwrite, to ensure that it will produce
    // correct metadata when it is written

    long currentMs = System.currentTimeMillis();
    String json =
        String.format(
            "{\n"
                + "  \"snapshot-id\" : 2,\n"
                + "  \"parent-snapshot-id\" : 1,\n"
                + "  \"timestamp-ms\" : %s,\n"
                + "  \"summary\" : {\n"
                + "    \"files-added\" : \"4\",\n"
                + "    \"files-deleted\" : \"100\"\n"
                + "  },\n"
                + "  \"manifests\" : [ \"/tmp/manifest1.avro\", \"/tmp/manifest2.avro\" ],\n"
                + "  \"schema-id\" : 3\n"
                + "}",
            currentMs);

    Snapshot snap = SnapshotParser.fromJson(json);
    String expected =
        String.format(
            "{\n"
                + "  \"snapshot-id\" : 2,\n"
                + "  \"parent-snapshot-id\" : 1,\n"
                + "  \"timestamp-ms\" : %s,\n"
                + "  \"summary\" : {\n"
                + "    \"operation\" : \"overwrite\",\n"
                + "    \"files-added\" : \"4\",\n"
                + "    \"files-deleted\" : \"100\"\n"
                + "  },\n"
                + "  \"manifests\" : [ \"/tmp/manifest1.avro\", \"/tmp/manifest2.avro\" ],\n"
                + "  \"schema-id\" : 3\n"
                + "}",
            currentMs);
    assertThat(SnapshotParser.toJson(snap)).isEqualTo(expected);
  }

  @Test
  public void testJsonConversionEmptySummary() {
    // This behavior is out of spec, but we don't want to fail on it.
    // Instead, when we find an empty summary, we'll just set it to null

    long currentMs = System.currentTimeMillis();
    String json =
        String.format(
            "{\n"
                + "  \"snapshot-id\" : 2,\n"
                + "  \"parent-snapshot-id\" : 1,\n"
                + "  \"timestamp-ms\" : %s,\n"
                + "  \"summary\" : { },\n"
                + "  \"manifests\" : [ \"/tmp/manifest1.avro\", \"/tmp/manifest2.avro\" ],\n"
                + "  \"schema-id\" : 3\n"
                + "}",
            currentMs);

    Snapshot snap = SnapshotParser.fromJson(json);
    String expected =
        String.format(
            "{\n"
                + "  \"snapshot-id\" : 2,\n"
                + "  \"parent-snapshot-id\" : 1,\n"
                + "  \"timestamp-ms\" : %s,\n"
                + "  \"manifests\" : [ \"/tmp/manifest1.avro\", \"/tmp/manifest2.avro\" ],\n"
                + "  \"schema-id\" : 3\n"
                + "}",
            currentMs);
    assertThat(SnapshotParser.toJson(snap)).isEqualTo(expected);
  }
}
