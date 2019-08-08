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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import org.apache.iceberg.TableMetadata.SnapshotLogEntry;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.JsonUtil;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.Files.localInput;
import static org.apache.iceberg.TableMetadataParser.CURRENT_SNAPSHOT_ID;
import static org.apache.iceberg.TableMetadataParser.FORMAT_VERSION;
import static org.apache.iceberg.TableMetadataParser.LAST_COLUMN_ID;
import static org.apache.iceberg.TableMetadataParser.LAST_UPDATED_MILLIS;
import static org.apache.iceberg.TableMetadataParser.LOCATION;
import static org.apache.iceberg.TableMetadataParser.PARTITION_SPEC;
import static org.apache.iceberg.TableMetadataParser.PROPERTIES;
import static org.apache.iceberg.TableMetadataParser.SCHEMA;
import static org.apache.iceberg.TableMetadataParser.SNAPSHOTS;

public class TestTableMetadataJson {
  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  public TableOperations ops = new LocalTableOperations(temp);

  @Test
  public void testJsonConversion() throws Exception {
    Schema schema = new Schema(
        Types.NestedField.required(1, "x", Types.LongType.get()),
        Types.NestedField.required(2, "y", Types.LongType.get(), "comment"),
        Types.NestedField.required(3, "z", Types.LongType.get())
    );

    PartitionSpec spec = PartitionSpec.builderFor(schema).withSpecId(5).build();

    long previousSnapshotId = System.currentTimeMillis() - new Random(1234).nextInt(3600);
    Snapshot previousSnapshot = new BaseSnapshot(
        ops, previousSnapshotId, null, previousSnapshotId, null, null, ImmutableList.of(
        new GenericManifestFile(localInput("file:/tmp/manfiest.1.avro"), spec.specId())));
    long currentSnapshotId = System.currentTimeMillis();
    Snapshot currentSnapshot = new BaseSnapshot(
        ops, currentSnapshotId, previousSnapshotId, currentSnapshotId, null, null, ImmutableList.of(
        new GenericManifestFile(localInput("file:/tmp/manfiest.2.avro"), spec.specId())));

    List<HistoryEntry> snapshotLog = ImmutableList.<HistoryEntry>builder()
        .add(new SnapshotLogEntry(previousSnapshot.timestampMillis(), previousSnapshot.snapshotId()))
        .add(new SnapshotLogEntry(currentSnapshot.timestampMillis(), currentSnapshot.snapshotId()))
        .build();

    TableMetadata expected = new TableMetadata(ops, null, UUID.randomUUID().toString(), "s3://bucket/test/location",
        System.currentTimeMillis(), 3, schema, 5, ImmutableList.of(spec),
        ImmutableMap.of("property", "value"), currentSnapshotId,
        Arrays.asList(previousSnapshot, currentSnapshot), snapshotLog);

    String asJson = TableMetadataParser.toJson(expected);
    TableMetadata metadata = TableMetadataParser.fromJson(ops, null,
        JsonUtil.mapper().readValue(asJson, JsonNode.class));

    Assert.assertEquals("Table UUID should match",
        expected.uuid(), metadata.uuid());
    Assert.assertEquals("Table location should match",
        expected.location(), metadata.location());
    Assert.assertEquals("Last column ID should match",
        expected.lastColumnId(), metadata.lastColumnId());
    Assert.assertEquals("Schema should match",
        expected.schema().asStruct(), metadata.schema().asStruct());
    Assert.assertEquals("Partition spec should match",
        expected.spec().toString(), metadata.spec().toString());
    Assert.assertEquals("Default spec ID should match",
        expected.defaultSpecId(), metadata.defaultSpecId());
    Assert.assertEquals("PartitionSpec map should match",
        expected.specs(), metadata.specs());
    Assert.assertEquals("Properties should match",
        expected.properties(), metadata.properties());
    Assert.assertEquals("Snapshot logs should match",
        expected.snapshotLog(), metadata.snapshotLog());
    Assert.assertEquals("Current snapshot ID should match",
        currentSnapshotId, metadata.currentSnapshot().snapshotId());
    Assert.assertEquals("Parent snapshot ID should match",
        (Long) previousSnapshotId, metadata.currentSnapshot().parentId());
    Assert.assertEquals("Current snapshot files should match",
        currentSnapshot.manifests(), metadata.currentSnapshot().manifests());
    Assert.assertEquals("Previous snapshot ID should match",
        previousSnapshotId, metadata.snapshot(previousSnapshotId).snapshotId());
    Assert.assertEquals("Previous snapshot files should match",
        previousSnapshot.manifests(),
        metadata.snapshot(previousSnapshotId).manifests());
  }

  @Test
  public void testFromJsonSortsSnapshotLog() throws Exception {
    Schema schema = new Schema(
        Types.NestedField.required(1, "x", Types.LongType.get()),
        Types.NestedField.required(2, "y", Types.LongType.get()),
        Types.NestedField.required(3, "z", Types.LongType.get())
    );

    PartitionSpec spec = PartitionSpec.builderFor(schema).withSpecId(5).build();

    long previousSnapshotId = System.currentTimeMillis() - new Random(1234).nextInt(3600);
    Snapshot previousSnapshot = new BaseSnapshot(
        ops, previousSnapshotId, null, previousSnapshotId, null, null, ImmutableList.of(
        new GenericManifestFile(localInput("file:/tmp/manfiest.1.avro"), spec.specId())));
    long currentSnapshotId = System.currentTimeMillis();
    Snapshot currentSnapshot = new BaseSnapshot(
        ops, currentSnapshotId, previousSnapshotId, currentSnapshotId, null, null, ImmutableList.of(
        new GenericManifestFile(localInput("file:/tmp/manfiest.2.avro"), spec.specId())));

    List<HistoryEntry> reversedSnapshotLog = Lists.newArrayList();

    TableMetadata expected = new TableMetadata(ops, null, UUID.randomUUID().toString(), "s3://bucket/test/location",
        System.currentTimeMillis(), 3, schema, 5, ImmutableList.of(spec),
        ImmutableMap.of("property", "value"), currentSnapshotId,
        Arrays.asList(previousSnapshot, currentSnapshot), reversedSnapshotLog);

    // add the entries after creating TableMetadata to avoid the sorted check
    reversedSnapshotLog.add(
        new SnapshotLogEntry(currentSnapshot.timestampMillis(), currentSnapshot.snapshotId()));
    reversedSnapshotLog.add(
        new SnapshotLogEntry(previousSnapshot.timestampMillis(), previousSnapshot.snapshotId()));

    String asJson = TableMetadataParser.toJson(expected);
    TableMetadata metadata = TableMetadataParser.fromJson(ops, null,
        JsonUtil.mapper().readValue(asJson, JsonNode.class));

    List<SnapshotLogEntry> expectedSnapshotLog = ImmutableList.<SnapshotLogEntry>builder()
        .add(new SnapshotLogEntry(previousSnapshot.timestampMillis(), previousSnapshot.snapshotId()))
        .add(new SnapshotLogEntry(currentSnapshot.timestampMillis(), currentSnapshot.snapshotId()))
        .build();

    Assert.assertEquals("Snapshot logs should match",
        expectedSnapshotLog, metadata.snapshotLog());
  }

  @Test
  public void testBackwardCompat() throws Exception {
    Schema schema = new Schema(
        Types.NestedField.required(1, "x", Types.LongType.get()),
        Types.NestedField.required(2, "y", Types.LongType.get()),
        Types.NestedField.required(3, "z", Types.LongType.get())
    );

    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("x").withSpecId(6).build();

    long previousSnapshotId = System.currentTimeMillis() - new Random(1234).nextInt(3600);
    Snapshot previousSnapshot = new BaseSnapshot(
        ops, previousSnapshotId, null, previousSnapshotId, null, null, ImmutableList.of(
        new GenericManifestFile(localInput("file:/tmp/manfiest.1.avro"), spec.specId())));
    long currentSnapshotId = System.currentTimeMillis();
    Snapshot currentSnapshot = new BaseSnapshot(
        ops, currentSnapshotId, previousSnapshotId, currentSnapshotId, null, null, ImmutableList.of(
        new GenericManifestFile(localInput("file:/tmp/manfiest.2.avro"), spec.specId())));

    TableMetadata expected = new TableMetadata(ops, null, null, "s3://bucket/test/location",
        System.currentTimeMillis(), 3, schema, 6, ImmutableList.of(spec),
        ImmutableMap.of("property", "value"), currentSnapshotId,
        Arrays.asList(previousSnapshot, currentSnapshot), ImmutableList.of());

    String asJson = toJsonWithoutSpecList(expected);
    TableMetadata metadata = TableMetadataParser
        .fromJson(ops, null, JsonUtil.mapper().readValue(asJson, JsonNode.class));

    Assert.assertNull("Table UUID should not be assigned", metadata.uuid());
    Assert.assertEquals("Table location should match",
        expected.location(), metadata.location());
    Assert.assertEquals("Last column ID should match",
        expected.lastColumnId(), metadata.lastColumnId());
    Assert.assertEquals("Schema should match",
        expected.schema().asStruct(), metadata.schema().asStruct());
    Assert.assertEquals("Partition spec should be the default",
        expected.spec().toString(), metadata.spec().toString());
    Assert.assertEquals("Default spec ID should default to TableMetadata.INITIAL_SPEC_ID",
        TableMetadata.INITIAL_SPEC_ID, metadata.defaultSpecId());
    Assert.assertEquals("PartitionSpec should contain the spec",
        1, metadata.specs().size());
    Assert.assertTrue("PartitionSpec should contain the spec",
        metadata.specs().get(0).compatibleWith(spec));
    Assert.assertEquals("PartitionSpec should have ID TableMetadata.INITIAL_SPEC_ID",
        TableMetadata.INITIAL_SPEC_ID, metadata.specs().get(0).specId());
    Assert.assertEquals("Properties should match",
        expected.properties(), metadata.properties());
    Assert.assertEquals("Snapshot logs should match",
        expected.snapshotLog(), metadata.snapshotLog());
    Assert.assertEquals("Current snapshot ID should match",
        currentSnapshotId, metadata.currentSnapshot().snapshotId());
    Assert.assertEquals("Parent snapshot ID should match",
        (Long) previousSnapshotId, metadata.currentSnapshot().parentId());
    Assert.assertEquals("Current snapshot files should match",
        currentSnapshot.manifests(), metadata.currentSnapshot().manifests());
    Assert.assertEquals("Previous snapshot ID should match",
        previousSnapshotId, metadata.snapshot(previousSnapshotId).snapshotId());
    Assert.assertEquals("Previous snapshot files should match",
        previousSnapshot.manifests(),
        metadata.snapshot(previousSnapshotId).manifests());
  }

  public static String toJsonWithoutSpecList(TableMetadata metadata) {
    StringWriter writer = new StringWriter();
    try {
      JsonGenerator generator = JsonUtil.factory().createGenerator(writer);

      generator.writeStartObject(); // start table metadata object

      generator.writeNumberField(FORMAT_VERSION, TableMetadata.TABLE_FORMAT_VERSION);
      generator.writeStringField(LOCATION, metadata.location());
      generator.writeNumberField(LAST_UPDATED_MILLIS, metadata.lastUpdatedMillis());
      generator.writeNumberField(LAST_COLUMN_ID, metadata.lastColumnId());

      generator.writeFieldName(SCHEMA);
      SchemaParser.toJson(metadata.schema(), generator);

      // mimic an old writer by writing only partition-spec and not the default ID or spec list
      generator.writeFieldName(PARTITION_SPEC);
      PartitionSpecParser.toJsonFields(metadata.spec(), generator);

      generator.writeObjectFieldStart(PROPERTIES);
      for (Map.Entry<String, String> keyValue : metadata.properties().entrySet()) {
        generator.writeStringField(keyValue.getKey(), keyValue.getValue());
      }
      generator.writeEndObject();

      generator.writeNumberField(CURRENT_SNAPSHOT_ID,
          metadata.currentSnapshot() != null ? metadata.currentSnapshot().snapshotId() : -1);

      generator.writeArrayFieldStart(SNAPSHOTS);
      for (Snapshot snapshot : metadata.snapshots()) {
        SnapshotParser.toJson(snapshot, generator);
      }
      generator.writeEndArray();

      // skip the snapshot log

      generator.writeEndObject(); // end table metadata object

      generator.flush();
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to write json for: %s", metadata);
    }
    return writer.toString();
  }
}
