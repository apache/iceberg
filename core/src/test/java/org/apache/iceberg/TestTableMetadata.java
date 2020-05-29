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
import java.io.IOException;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.UUID;
import org.apache.iceberg.TableMetadata.MetadataLogEntry;
import org.apache.iceberg.TableMetadata.SnapshotLogEntry;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
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
import static org.apache.iceberg.TableMetadataParser.TABLE_UUID;

public class TestTableMetadata {
  private static final String TEST_LOCATION = "s3://bucket/test/location";

  private static final Schema TEST_SCHEMA = new Schema(
      Types.NestedField.required(1, "x", Types.LongType.get()),
      Types.NestedField.required(2, "y", Types.LongType.get(), "comment"),
      Types.NestedField.required(3, "z", Types.LongType.get())
  );

  private static final long SEQ_NO = 34;
  private static final int LAST_ASSIGNED_COLUMN_ID = 3;

  private static final PartitionSpec SPEC_5 = PartitionSpec.builderFor(TEST_SCHEMA).withSpecId(5).build();

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  public TableOperations ops = new LocalTableOperations(temp);

  @Test
  public void testJsonConversion() throws Exception {
    long previousSnapshotId = System.currentTimeMillis() - new Random(1234).nextInt(3600);
    Snapshot previousSnapshot = new BaseSnapshot(
        ops.io(), previousSnapshotId, null, previousSnapshotId, null, null, ImmutableList.of(
        new GenericManifestFile(localInput("file:/tmp/manfiest.1.avro"), SPEC_5.specId())));
    long currentSnapshotId = System.currentTimeMillis();
    Snapshot currentSnapshot = new BaseSnapshot(
        ops.io(), currentSnapshotId, previousSnapshotId, currentSnapshotId, null, null, ImmutableList.of(
        new GenericManifestFile(localInput("file:/tmp/manfiest.2.avro"), SPEC_5.specId())));

    List<HistoryEntry> snapshotLog = ImmutableList.<HistoryEntry>builder()
        .add(new SnapshotLogEntry(previousSnapshot.timestampMillis(), previousSnapshot.snapshotId()))
        .add(new SnapshotLogEntry(currentSnapshot.timestampMillis(), currentSnapshot.snapshotId()))
        .build();

    TableMetadata expected = new TableMetadata(null, 2, UUID.randomUUID().toString(), TEST_LOCATION,
        SEQ_NO, System.currentTimeMillis(), 3, TEST_SCHEMA, 5, ImmutableList.of(SPEC_5),
        ImmutableMap.of("property", "value"), currentSnapshotId,
        Arrays.asList(previousSnapshot, currentSnapshot), snapshotLog, ImmutableList.of());

    String asJson = TableMetadataParser.toJson(expected);
    TableMetadata metadata = TableMetadataParser.fromJson(ops.io(), null,
        JsonUtil.mapper().readValue(asJson, JsonNode.class));

    Assert.assertEquals("Format version should match",
        expected.formatVersion(), metadata.formatVersion());
    Assert.assertEquals("Table UUID should match",
        expected.uuid(), metadata.uuid());
    Assert.assertEquals("Table location should match",
        expected.location(), metadata.location());
    Assert.assertEquals("Last sequence number should match",
        expected.lastSequenceNumber(), metadata.lastSequenceNumber());
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
    long previousSnapshotId = System.currentTimeMillis() - new Random(1234).nextInt(3600);
    Snapshot previousSnapshot = new BaseSnapshot(
        ops.io(), previousSnapshotId, null, previousSnapshotId, null, null, ImmutableList.of(
        new GenericManifestFile(localInput("file:/tmp/manfiest.1.avro"), SPEC_5.specId())));
    long currentSnapshotId = System.currentTimeMillis();
    Snapshot currentSnapshot = new BaseSnapshot(
        ops.io(), currentSnapshotId, previousSnapshotId, currentSnapshotId, null, null, ImmutableList.of(
        new GenericManifestFile(localInput("file:/tmp/manfiest.2.avro"), SPEC_5.specId())));

    List<HistoryEntry> reversedSnapshotLog = Lists.newArrayList();

    TableMetadata expected = new TableMetadata(null, 1, UUID.randomUUID().toString(), TEST_LOCATION,
        0, System.currentTimeMillis(), 3, TEST_SCHEMA, 5, ImmutableList.of(SPEC_5),
        ImmutableMap.of("property", "value"), currentSnapshotId,
        Arrays.asList(previousSnapshot, currentSnapshot), reversedSnapshotLog, ImmutableList.of());

    // add the entries after creating TableMetadata to avoid the sorted check
    reversedSnapshotLog.add(
        new SnapshotLogEntry(currentSnapshot.timestampMillis(), currentSnapshot.snapshotId()));
    reversedSnapshotLog.add(
        new SnapshotLogEntry(previousSnapshot.timestampMillis(), previousSnapshot.snapshotId()));

    String asJson = TableMetadataParser.toJson(expected);
    TableMetadata metadata = TableMetadataParser.fromJson(ops.io(), null,
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
    PartitionSpec spec = PartitionSpec.builderFor(TEST_SCHEMA).identity("x").withSpecId(6).build();

    long previousSnapshotId = System.currentTimeMillis() - new Random(1234).nextInt(3600);
    Snapshot previousSnapshot = new BaseSnapshot(
        ops.io(), previousSnapshotId, null, previousSnapshotId, null, null, ImmutableList.of(
        new GenericManifestFile(localInput("file:/tmp/manfiest.1.avro"), spec.specId())));
    long currentSnapshotId = System.currentTimeMillis();
    Snapshot currentSnapshot = new BaseSnapshot(
        ops.io(), currentSnapshotId, previousSnapshotId, currentSnapshotId, null, null, ImmutableList.of(
        new GenericManifestFile(localInput("file:/tmp/manfiest.2.avro"), spec.specId())));

    TableMetadata expected = new TableMetadata(null, 1, null, TEST_LOCATION,
        0, System.currentTimeMillis(), 3, TEST_SCHEMA, 6, ImmutableList.of(spec),
        ImmutableMap.of("property", "value"), currentSnapshotId,
        Arrays.asList(previousSnapshot, currentSnapshot), ImmutableList.of(), ImmutableList.of());

    String asJson = toJsonWithoutSpecList(expected);
    TableMetadata metadata = TableMetadataParser
        .fromJson(ops.io(), null, JsonUtil.mapper().readValue(asJson, JsonNode.class));

    Assert.assertEquals("Format version should match",
        expected.formatVersion(), metadata.formatVersion());
    Assert.assertNull("Table UUID should not be assigned", metadata.uuid());
    Assert.assertEquals("Table location should match",
        expected.location(), metadata.location());
    Assert.assertEquals("Last sequence number should default to 0",
        expected.lastSequenceNumber(), metadata.lastSequenceNumber());
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
    Assert.assertEquals("Snapshot logs should match",
            expected.previousFiles(), metadata.previousFiles());
  }

  public static String toJsonWithoutSpecList(TableMetadata metadata) {
    StringWriter writer = new StringWriter();
    try {
      JsonGenerator generator = JsonUtil.factory().createGenerator(writer);

      generator.writeStartObject(); // start table metadata object

      generator.writeNumberField(FORMAT_VERSION, 1);
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

  @Test
  public void testJsonWithPreviousMetadataLog() throws Exception {
    long previousSnapshotId = System.currentTimeMillis() - new Random(1234).nextInt(3600);
    Snapshot previousSnapshot = new BaseSnapshot(
        ops.io(), previousSnapshotId, null, previousSnapshotId, null, null, ImmutableList.of(
        new GenericManifestFile(localInput("file:/tmp/manfiest.1.avro"), SPEC_5.specId())));
    long currentSnapshotId = System.currentTimeMillis();
    Snapshot currentSnapshot = new BaseSnapshot(
        ops.io(), currentSnapshotId, previousSnapshotId, currentSnapshotId, null, null, ImmutableList.of(
        new GenericManifestFile(localInput("file:/tmp/manfiest.2.avro"), SPEC_5.specId())));

    List<HistoryEntry> reversedSnapshotLog = Lists.newArrayList();
    long currentTimestamp = System.currentTimeMillis();
    List<MetadataLogEntry> previousMetadataLog = Lists.newArrayList();
    previousMetadataLog.add(new MetadataLogEntry(currentTimestamp,
        "/tmp/000001-" + UUID.randomUUID().toString() + ".metadata.json"));

    TableMetadata base = new TableMetadata(null, 1, UUID.randomUUID().toString(), TEST_LOCATION,
        0, System.currentTimeMillis(), 3, TEST_SCHEMA, 5, ImmutableList.of(SPEC_5),
        ImmutableMap.of("property", "value"), currentSnapshotId,
        Arrays.asList(previousSnapshot, currentSnapshot), reversedSnapshotLog,
        ImmutableList.copyOf(previousMetadataLog));

    String asJson = TableMetadataParser.toJson(base);
    TableMetadata metadataFromJson = TableMetadataParser.fromJson(ops.io(), null,
        JsonUtil.mapper().readValue(asJson, JsonNode.class));

    Assert.assertEquals("Metadata logs should match", previousMetadataLog, metadataFromJson.previousFiles());
  }

  @Test
  public void testAddPreviousMetadataRemoveNone() {
    long previousSnapshotId = System.currentTimeMillis() - new Random(1234).nextInt(3600);
    Snapshot previousSnapshot = new BaseSnapshot(
        ops.io(), previousSnapshotId, null, previousSnapshotId, null, null, ImmutableList.of(
        new GenericManifestFile(localInput("file:/tmp/manfiest.1.avro"), SPEC_5.specId())));
    long currentSnapshotId = System.currentTimeMillis();
    Snapshot currentSnapshot = new BaseSnapshot(
        ops.io(), currentSnapshotId, previousSnapshotId, currentSnapshotId, null, null, ImmutableList.of(
        new GenericManifestFile(localInput("file:/tmp/manfiest.2.avro"), SPEC_5.specId())));

    List<HistoryEntry> reversedSnapshotLog = Lists.newArrayList();
    long currentTimestamp = System.currentTimeMillis();
    List<MetadataLogEntry> previousMetadataLog = Lists.newArrayList();
    previousMetadataLog.add(new MetadataLogEntry(currentTimestamp - 100,
        "/tmp/000001-" + UUID.randomUUID().toString() + ".metadata.json"));
    previousMetadataLog.add(new MetadataLogEntry(currentTimestamp - 90,
        "/tmp/000002-" + UUID.randomUUID().toString() + ".metadata.json"));

    MetadataLogEntry latestPreviousMetadata = new MetadataLogEntry(currentTimestamp - 80,
        "/tmp/000003-" + UUID.randomUUID().toString() + ".metadata.json");

    TableMetadata base = new TableMetadata(localInput(latestPreviousMetadata.file()), 1, UUID.randomUUID().toString(),
        TEST_LOCATION, 0, currentTimestamp - 80, 3, TEST_SCHEMA, 5, ImmutableList.of(SPEC_5),
        ImmutableMap.of("property", "value"), currentSnapshotId,
        Arrays.asList(previousSnapshot, currentSnapshot), reversedSnapshotLog,
        ImmutableList.copyOf(previousMetadataLog));

    previousMetadataLog.add(latestPreviousMetadata);

    TableMetadata metadata = base.replaceProperties(
        ImmutableMap.of(TableProperties.METADATA_PREVIOUS_VERSIONS_MAX, "5"));
    Set<MetadataLogEntry> removedPreviousMetadata = Sets.newHashSet(base.previousFiles());
    removedPreviousMetadata.removeAll(metadata.previousFiles());

    Assert.assertEquals("Metadata logs should match", previousMetadataLog, metadata.previousFiles());
    Assert.assertEquals("Removed Metadata logs should be empty", 0, removedPreviousMetadata.size());
  }

  @Test
  public void testAddPreviousMetadataRemoveOne() {
    long previousSnapshotId = System.currentTimeMillis() - new Random(1234).nextInt(3600);
    Snapshot previousSnapshot = new BaseSnapshot(
        ops.io(), previousSnapshotId, null, previousSnapshotId, null, null, ImmutableList.of(
        new GenericManifestFile(localInput("file:/tmp/manfiest.1.avro"), SPEC_5.specId())));
    long currentSnapshotId = System.currentTimeMillis();
    Snapshot currentSnapshot = new BaseSnapshot(
        ops.io(), currentSnapshotId, previousSnapshotId, currentSnapshotId, null, null, ImmutableList.of(
        new GenericManifestFile(localInput("file:/tmp/manfiest.2.avro"), SPEC_5.specId())));

    List<HistoryEntry> reversedSnapshotLog = Lists.newArrayList();
    long currentTimestamp = System.currentTimeMillis();
    List<MetadataLogEntry> previousMetadataLog = Lists.newArrayList();
    previousMetadataLog.add(new MetadataLogEntry(currentTimestamp - 100,
        "/tmp/000001-" + UUID.randomUUID().toString() + ".metadata.json"));
    previousMetadataLog.add(new MetadataLogEntry(currentTimestamp - 90,
        "/tmp/000002-" + UUID.randomUUID().toString() + ".metadata.json"));
    previousMetadataLog.add(new MetadataLogEntry(currentTimestamp - 80,
        "/tmp/000003-" + UUID.randomUUID().toString() + ".metadata.json"));
    previousMetadataLog.add(new MetadataLogEntry(currentTimestamp - 70,
        "/tmp/000004-" + UUID.randomUUID().toString() + ".metadata.json"));
    previousMetadataLog.add(new MetadataLogEntry(currentTimestamp - 60,
        "/tmp/000005-" + UUID.randomUUID().toString() + ".metadata.json"));

    MetadataLogEntry latestPreviousMetadata = new MetadataLogEntry(currentTimestamp - 50,
        "/tmp/000006-" + UUID.randomUUID().toString() + ".metadata.json");

    TableMetadata base = new TableMetadata(localInput(latestPreviousMetadata.file()), 1, UUID.randomUUID().toString(),
        TEST_LOCATION, 0, currentTimestamp - 50, 3, TEST_SCHEMA, 5,
        ImmutableList.of(SPEC_5), ImmutableMap.of("property", "value"), currentSnapshotId,
        Arrays.asList(previousSnapshot, currentSnapshot), reversedSnapshotLog,
        ImmutableList.copyOf(previousMetadataLog));

    previousMetadataLog.add(latestPreviousMetadata);

    TableMetadata metadata = base.replaceProperties(
        ImmutableMap.of(TableProperties.METADATA_PREVIOUS_VERSIONS_MAX, "5"));

    SortedSet<MetadataLogEntry> removedPreviousMetadata =
        Sets.newTreeSet(Comparator.comparingLong(MetadataLogEntry::timestampMillis));
    removedPreviousMetadata.addAll(base.previousFiles());
    removedPreviousMetadata.removeAll(metadata.previousFiles());

    Assert.assertEquals("Metadata logs should match", previousMetadataLog.subList(1, 6),
        metadata.previousFiles());
    Assert.assertEquals("Removed Metadata logs should contain 1", previousMetadataLog.subList(0, 1),
        ImmutableList.copyOf(removedPreviousMetadata));
  }

  @Test
  public void testAddPreviousMetadataRemoveMultiple() {
    long previousSnapshotId = System.currentTimeMillis() - new Random(1234).nextInt(3600);
    Snapshot previousSnapshot = new BaseSnapshot(
        ops.io(), previousSnapshotId, null, previousSnapshotId, null, null, ImmutableList.of(
        new GenericManifestFile(localInput("file:/tmp/manfiest.1.avro"), SPEC_5.specId())));
    long currentSnapshotId = System.currentTimeMillis();
    Snapshot currentSnapshot = new BaseSnapshot(
        ops.io(), currentSnapshotId, previousSnapshotId, currentSnapshotId, null, null, ImmutableList.of(
        new GenericManifestFile(localInput("file:/tmp/manfiest.2.avro"), SPEC_5.specId())));

    List<HistoryEntry> reversedSnapshotLog = Lists.newArrayList();
    long currentTimestamp = System.currentTimeMillis();
    List<MetadataLogEntry> previousMetadataLog = Lists.newArrayList();
    previousMetadataLog.add(new MetadataLogEntry(currentTimestamp - 100,
        "/tmp/000001-" + UUID.randomUUID().toString() + ".metadata.json"));
    previousMetadataLog.add(new MetadataLogEntry(currentTimestamp - 90,
        "/tmp/000002-" + UUID.randomUUID().toString() + ".metadata.json"));
    previousMetadataLog.add(new MetadataLogEntry(currentTimestamp - 80,
        "/tmp/000003-" + UUID.randomUUID().toString() + ".metadata.json"));
    previousMetadataLog.add(new MetadataLogEntry(currentTimestamp - 70,
        "/tmp/000004-" + UUID.randomUUID().toString() + ".metadata.json"));
    previousMetadataLog.add(new MetadataLogEntry(currentTimestamp - 60,
        "/tmp/000005-" + UUID.randomUUID().toString() + ".metadata.json"));

    MetadataLogEntry latestPreviousMetadata = new MetadataLogEntry(currentTimestamp - 50,
        "/tmp/000006-" + UUID.randomUUID().toString() + ".metadata.json");

    TableMetadata base = new TableMetadata(localInput(latestPreviousMetadata.file()), 1, UUID.randomUUID().toString(),
        TEST_LOCATION, 0, currentTimestamp - 50, 3, TEST_SCHEMA, 2,
        ImmutableList.of(SPEC_5), ImmutableMap.of("property", "value"), currentSnapshotId,
        Arrays.asList(previousSnapshot, currentSnapshot), reversedSnapshotLog,
        ImmutableList.copyOf(previousMetadataLog));

    previousMetadataLog.add(latestPreviousMetadata);

    TableMetadata metadata = base.replaceProperties(
        ImmutableMap.of(TableProperties.METADATA_PREVIOUS_VERSIONS_MAX, "2"));

    SortedSet<MetadataLogEntry> removedPreviousMetadata =
        Sets.newTreeSet(Comparator.comparingLong(MetadataLogEntry::timestampMillis));
    removedPreviousMetadata.addAll(base.previousFiles());
    removedPreviousMetadata.removeAll(metadata.previousFiles());

    Assert.assertEquals("Metadata logs should match", previousMetadataLog.subList(4, 6),
        metadata.previousFiles());
    Assert.assertEquals("Removed Metadata logs should contain 4", previousMetadataLog.subList(0, 4),
        ImmutableList.copyOf(removedPreviousMetadata));
  }

  @Test
  public void testV2UUIDValidation() {
    AssertHelpers.assertThrows("Should reject v2 metadata without a UUID",
        IllegalArgumentException.class, "UUID is required in format v2",
        () -> new TableMetadata(null, 2, null, TEST_LOCATION, SEQ_NO, System.currentTimeMillis(),
            LAST_ASSIGNED_COLUMN_ID, TEST_SCHEMA, SPEC_5.specId(), ImmutableList.of(SPEC_5), ImmutableMap.of(), -1L,
            ImmutableList.of(), ImmutableList.of(), ImmutableList.of())
    );
  }

  @Test
  public void testVersionValidation() {
    int unsupportedVersion = TableMetadata.SUPPORTED_TABLE_FORMAT_VERSION + 1;
    AssertHelpers.assertThrows("Should reject unsupported metadata",
        IllegalArgumentException.class, "Unsupported format version: v" + unsupportedVersion,
        () -> new TableMetadata(null, unsupportedVersion, null, TEST_LOCATION, SEQ_NO,
            System.currentTimeMillis(), LAST_ASSIGNED_COLUMN_ID, TEST_SCHEMA, SPEC_5.specId(), ImmutableList.of(SPEC_5),
            ImmutableMap.of(), -1L, ImmutableList.of(), ImmutableList.of(), ImmutableList.of())
    );
  }

  @Test
  public void testParserVersionValidation() throws Exception {
    String supportedVersion = toJsonWithVersion(
        TableMetadata.newTableMetadata(TEST_SCHEMA, SPEC_5, TEST_LOCATION, ImmutableMap.of()),
        TableMetadata.SUPPORTED_TABLE_FORMAT_VERSION);
    TableMetadata parsed = TableMetadataParser.fromJson(
        ops.io(), null, JsonUtil.mapper().readValue(supportedVersion, JsonNode.class));
    Assert.assertNotNull("Should successfully read supported metadata version", parsed);

    String unsupportedVersion = toJsonWithVersion(
        TableMetadata.newTableMetadata(TEST_SCHEMA, SPEC_5, TEST_LOCATION, ImmutableMap.of()),
        TableMetadata.SUPPORTED_TABLE_FORMAT_VERSION + 1);
    AssertHelpers.assertThrows("Should not read unsupported metadata",
        IllegalArgumentException.class, "Cannot read unsupported version",
        () -> TableMetadataParser.fromJson(
            ops.io(), null, JsonUtil.mapper().readValue(unsupportedVersion, JsonNode.class)));
  }

  public static String toJsonWithVersion(TableMetadata metadata, int version) {
    StringWriter writer = new StringWriter();
    try {
      JsonGenerator generator = JsonUtil.factory().createGenerator(writer);

      generator.writeStartObject(); // start table metadata object

      generator.writeNumberField(FORMAT_VERSION, version);
      generator.writeStringField(TABLE_UUID, metadata.uuid());
      generator.writeStringField(LOCATION, metadata.location());
      generator.writeNumberField(LAST_UPDATED_MILLIS, metadata.lastUpdatedMillis());
      if (version > 1) {
        generator.writeNumberField(TableMetadataParser.LAST_SEQUENCE_NUMBER, metadata.lastSequenceNumber());
      }
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

  @Test
  public void testNewTableMetadataReassignmentAllIds() throws Exception {
    Schema schema = new Schema(
        Types.NestedField.required(3, "x", Types.LongType.get()),
        Types.NestedField.required(4, "y", Types.LongType.get()),
        Types.NestedField.required(5, "z", Types.LongType.get())
    );

    PartitionSpec spec = PartitionSpec.builderFor(schema).withSpecId(5)
        .add(3, 1005, "x_partition", "bucket[4]")
        .add(5, 1005, "z_partition", "bucket[8]")
        .build();
    String location = "file://tmp/db/table";
    TableMetadata metadata = TableMetadata.newTableMetadata(schema, spec, location, ImmutableMap.of());

    // newTableMetadata should reassign column ids and partition field ids.
    PartitionSpec expected = PartitionSpec.builderFor(metadata.schema()).withSpecId(0)
        .add(1, 1000, "x_partition", "bucket[4]")
        .add(3, 1001, "z_partition", "bucket[8]")
        .build();

    Assert.assertEquals(expected, metadata.spec());
  }

  @Test
  public void testInvalidUpdatePartitionSpecForV1Table() throws Exception {
    Schema schema = new Schema(
        Types.NestedField.required(1, "x", Types.LongType.get())
    );

    PartitionSpec spec = PartitionSpec.builderFor(schema).withSpecId(5)
        .add(1, 1005, "x_partition", "bucket[4]")
        .build();
    String location = "file://tmp/db/table";
    TableMetadata metadata = TableMetadata.newTableMetadata(schema, spec, location, ImmutableMap.of());

    AssertHelpers.assertThrows("Should fail to update an invalid partition spec",
        ValidationException.class, "Spec does not use sequential IDs that are required in v1",
        () -> metadata.updatePartitionSpec(spec));
  }
}
