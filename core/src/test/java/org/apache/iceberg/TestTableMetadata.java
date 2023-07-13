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
import static org.apache.iceberg.TestHelpers.assertSameSchemaList;

import com.fasterxml.jackson.core.JsonGenerator;
import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.UUID;
import org.apache.iceberg.TableMetadata.MetadataLogEntry;
import org.apache.iceberg.TableMetadata.SnapshotLogEntry;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.JsonUtil;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestTableMetadata {
  private static final String TEST_LOCATION = "s3://bucket/test/location";

  private static final Schema TEST_SCHEMA =
      new Schema(
          7,
          Types.NestedField.required(1, "x", Types.LongType.get()),
          Types.NestedField.required(2, "y", Types.LongType.get(), "comment"),
          Types.NestedField.required(3, "z", Types.LongType.get()));

  private static final long SEQ_NO = 34;
  private static final int LAST_ASSIGNED_COLUMN_ID = 3;

  private static final PartitionSpec SPEC_5 =
      PartitionSpec.builderFor(TEST_SCHEMA).withSpecId(5).build();
  private static final SortOrder SORT_ORDER_3 =
      SortOrder.builderFor(TEST_SCHEMA)
          .withOrderId(3)
          .asc("y", NullOrder.NULLS_FIRST)
          .desc(Expressions.bucket("z", 4), NullOrder.NULLS_LAST)
          .build();

  @Rule public TemporaryFolder temp = new TemporaryFolder();

  public TableOperations ops = new LocalTableOperations(temp);

  @Test
  public void testJsonConversion() throws Exception {
    long previousSnapshotId = System.currentTimeMillis() - new Random(1234).nextInt(3600);

    String manifestList =
        createManifestListWithManifestFile(previousSnapshotId, null, "file:/tmp/manifest1.avro");
    Snapshot previousSnapshot =
        new BaseSnapshot(
            0, previousSnapshotId, null, previousSnapshotId, null, null, null, manifestList);

    long currentSnapshotId = System.currentTimeMillis();
    manifestList =
        createManifestListWithManifestFile(
            currentSnapshotId, previousSnapshotId, "file:/tmp/manifest2.avro");
    Snapshot currentSnapshot =
        new BaseSnapshot(
            0,
            currentSnapshotId,
            previousSnapshotId,
            currentSnapshotId,
            null,
            null,
            7,
            manifestList);

    List<HistoryEntry> snapshotLog =
        ImmutableList.<HistoryEntry>builder()
            .add(
                new SnapshotLogEntry(
                    previousSnapshot.timestampMillis(), previousSnapshot.snapshotId()))
            .add(
                new SnapshotLogEntry(
                    currentSnapshot.timestampMillis(), currentSnapshot.snapshotId()))
            .build();

    Schema schema = new Schema(6, Types.NestedField.required(10, "x", Types.StringType.get()));

    Map<String, SnapshotRef> refs =
        ImmutableMap.of(
            "main", SnapshotRef.branchBuilder(currentSnapshotId).build(),
            "previous", SnapshotRef.tagBuilder(previousSnapshotId).build(),
            "test", SnapshotRef.branchBuilder(previousSnapshotId).build());

    List<StatisticsFile> statisticsFiles =
        ImmutableList.of(
            new GenericStatisticsFile(
                11L,
                "/some/stats/file.puffin",
                100,
                42,
                ImmutableList.of(
                    new GenericBlobMetadata(
                        "some-stats", 11L, 2, ImmutableList.of(4), ImmutableMap.of()))));

    TableMetadata expected =
        new TableMetadata(
            null,
            2,
            UUID.randomUUID().toString(),
            TEST_LOCATION,
            SEQ_NO,
            System.currentTimeMillis(),
            3,
            7,
            ImmutableList.of(TEST_SCHEMA, schema),
            5,
            ImmutableList.of(SPEC_5),
            SPEC_5.lastAssignedFieldId(),
            3,
            ImmutableList.of(SORT_ORDER_3),
            ImmutableMap.of("property", "value"),
            currentSnapshotId,
            Arrays.asList(previousSnapshot, currentSnapshot),
            null,
            snapshotLog,
            ImmutableList.of(),
            refs,
            statisticsFiles,
            ImmutableList.of());

    String asJson = TableMetadataParser.toJson(expected);
    TableMetadata metadata = TableMetadataParser.fromJson(asJson);

    Assert.assertEquals(
        "Format version should match", expected.formatVersion(), metadata.formatVersion());
    Assert.assertEquals("Table UUID should match", expected.uuid(), metadata.uuid());
    Assert.assertEquals("Table location should match", expected.location(), metadata.location());
    Assert.assertEquals(
        "Last sequence number should match",
        expected.lastSequenceNumber(),
        metadata.lastSequenceNumber());
    Assert.assertEquals(
        "Last column ID should match", expected.lastColumnId(), metadata.lastColumnId());
    Assert.assertEquals(
        "Current schema id should match", expected.currentSchemaId(), metadata.currentSchemaId());
    assertSameSchemaList(expected.schemas(), metadata.schemas());
    Assert.assertEquals(
        "Partition spec should match", expected.spec().toString(), metadata.spec().toString());
    Assert.assertEquals(
        "Default spec ID should match", expected.defaultSpecId(), metadata.defaultSpecId());
    Assert.assertEquals("PartitionSpec map should match", expected.specs(), metadata.specs());
    Assert.assertEquals(
        "lastAssignedFieldId across all PartitionSpecs should match",
        expected.spec().lastAssignedFieldId(),
        metadata.lastAssignedPartitionId());
    Assert.assertEquals(
        "Default sort ID should match",
        expected.defaultSortOrderId(),
        metadata.defaultSortOrderId());
    Assert.assertEquals("Sort order should match", expected.sortOrder(), metadata.sortOrder());
    Assert.assertEquals(
        "Sort order map should match", expected.sortOrders(), metadata.sortOrders());
    Assert.assertEquals("Properties should match", expected.properties(), metadata.properties());
    Assert.assertEquals(
        "Snapshot logs should match", expected.snapshotLog(), metadata.snapshotLog());
    Assert.assertEquals(
        "Current snapshot ID should match",
        currentSnapshotId,
        metadata.currentSnapshot().snapshotId());
    Assert.assertEquals(
        "Parent snapshot ID should match",
        (Long) previousSnapshotId,
        metadata.currentSnapshot().parentId());
    Assert.assertEquals(
        "Current snapshot files should match",
        currentSnapshot.allManifests(ops.io()),
        metadata.currentSnapshot().allManifests(ops.io()));
    Assert.assertEquals(
        "Schema ID for current snapshot should match",
        (Integer) 7,
        metadata.currentSnapshot().schemaId());
    Assert.assertEquals(
        "Previous snapshot ID should match",
        previousSnapshotId,
        metadata.snapshot(previousSnapshotId).snapshotId());
    Assert.assertEquals(
        "Previous snapshot files should match",
        previousSnapshot.allManifests(ops.io()),
        metadata.snapshot(previousSnapshotId).allManifests(ops.io()));
    Assert.assertNull(
        "Previous snapshot's schema ID should be null",
        metadata.snapshot(previousSnapshotId).schemaId());
    Assert.assertEquals(
        "Statistics files should match", statisticsFiles, metadata.statisticsFiles());
    Assert.assertEquals("Refs map should match", refs, metadata.refs());
  }

  @Test
  public void testBackwardCompat() throws Exception {
    PartitionSpec spec = PartitionSpec.builderFor(TEST_SCHEMA).identity("x").withSpecId(6).build();
    SortOrder sortOrder = SortOrder.unsorted();
    Schema schema = new Schema(TableMetadata.INITIAL_SCHEMA_ID, TEST_SCHEMA.columns());

    long previousSnapshotId = System.currentTimeMillis() - new Random(1234).nextInt(3600);

    String manifestList =
        createManifestListWithManifestFile(previousSnapshotId, null, "file:/tmp/manifest1.avro");
    Snapshot previousSnapshot =
        new BaseSnapshot(
            0, previousSnapshotId, null, previousSnapshotId, null, null, null, manifestList);

    long currentSnapshotId = System.currentTimeMillis();
    manifestList =
        createManifestListWithManifestFile(
            currentSnapshotId, previousSnapshotId, "file:/tmp/manifest2.avro");
    Snapshot currentSnapshot =
        new BaseSnapshot(
            0,
            currentSnapshotId,
            previousSnapshotId,
            currentSnapshotId,
            null,
            null,
            null,
            manifestList);

    TableMetadata expected =
        new TableMetadata(
            null,
            1,
            null,
            TEST_LOCATION,
            0,
            System.currentTimeMillis(),
            3,
            TableMetadata.INITIAL_SCHEMA_ID,
            ImmutableList.of(schema),
            6,
            ImmutableList.of(spec),
            spec.lastAssignedFieldId(),
            TableMetadata.INITIAL_SORT_ORDER_ID,
            ImmutableList.of(sortOrder),
            ImmutableMap.of("property", "value"),
            currentSnapshotId,
            Arrays.asList(previousSnapshot, currentSnapshot),
            null,
            ImmutableList.of(),
            ImmutableList.of(),
            ImmutableMap.of(),
            ImmutableList.of(),
            ImmutableList.of());

    String asJson = toJsonWithoutSpecAndSchemaList(expected);
    TableMetadata metadata = TableMetadataParser.fromJson(asJson);

    Assert.assertEquals(
        "Format version should match", expected.formatVersion(), metadata.formatVersion());
    Assert.assertNull("Table UUID should not be assigned", metadata.uuid());
    Assert.assertEquals("Table location should match", expected.location(), metadata.location());
    Assert.assertEquals(
        "Last sequence number should default to 0",
        expected.lastSequenceNumber(),
        metadata.lastSequenceNumber());
    Assert.assertEquals(
        "Last column ID should match", expected.lastColumnId(), metadata.lastColumnId());
    Assert.assertEquals(
        "Current schema ID should be default to TableMetadata.INITIAL_SCHEMA_ID",
        TableMetadata.INITIAL_SCHEMA_ID,
        metadata.currentSchemaId());
    Assert.assertEquals("Schemas size should match", 1, metadata.schemas().size());
    Assert.assertEquals(
        "Schemas should contain the schema",
        metadata.schemas().get(0).asStruct(),
        schema.asStruct());
    Assert.assertEquals(
        "Partition spec should be the default",
        expected.spec().toString(),
        metadata.spec().toString());
    Assert.assertEquals(
        "Default spec ID should default to TableMetadata.INITIAL_SPEC_ID",
        TableMetadata.INITIAL_SPEC_ID,
        metadata.defaultSpecId());
    Assert.assertEquals("PartitionSpec should contain the spec", 1, metadata.specs().size());
    Assert.assertTrue(
        "PartitionSpec should contain the spec", metadata.specs().get(0).compatibleWith(spec));
    Assert.assertEquals(
        "PartitionSpec should have ID TableMetadata.INITIAL_SPEC_ID",
        TableMetadata.INITIAL_SPEC_ID,
        metadata.specs().get(0).specId());
    Assert.assertEquals(
        "lastAssignedFieldId across all PartitionSpecs should match",
        expected.spec().lastAssignedFieldId(),
        metadata.lastAssignedPartitionId());
    Assert.assertEquals("Properties should match", expected.properties(), metadata.properties());
    Assert.assertEquals(
        "Snapshot logs should match", expected.snapshotLog(), metadata.snapshotLog());
    Assert.assertEquals(
        "Current snapshot ID should match",
        currentSnapshotId,
        metadata.currentSnapshot().snapshotId());
    Assert.assertEquals(
        "Parent snapshot ID should match",
        (Long) previousSnapshotId,
        metadata.currentSnapshot().parentId());
    Assert.assertEquals(
        "Current snapshot files should match",
        currentSnapshot.allManifests(ops.io()),
        metadata.currentSnapshot().allManifests(ops.io()));
    Assert.assertNull(
        "Current snapshot's schema ID should be null", metadata.currentSnapshot().schemaId());
    Assert.assertEquals(
        "Previous snapshot ID should match",
        previousSnapshotId,
        metadata.snapshot(previousSnapshotId).snapshotId());
    Assert.assertEquals(
        "Previous snapshot files should match",
        previousSnapshot.allManifests(ops.io()),
        metadata.snapshot(previousSnapshotId).allManifests(ops.io()));
    Assert.assertEquals(
        "Snapshot logs should match", expected.previousFiles(), metadata.previousFiles());
    Assert.assertNull(
        "Previous snapshot's schema ID should be null",
        metadata.snapshot(previousSnapshotId).schemaId());
  }

  @Test
  public void testInvalidMainBranch() throws IOException {
    long previousSnapshotId = System.currentTimeMillis() - new Random(1234).nextInt(3600);

    String manifestList =
        createManifestListWithManifestFile(previousSnapshotId, null, "file:/tmp/manifest1.avro");
    Snapshot previousSnapshot =
        new BaseSnapshot(
            0, previousSnapshotId, null, previousSnapshotId, null, null, null, manifestList);

    long currentSnapshotId = System.currentTimeMillis();
    manifestList =
        createManifestListWithManifestFile(
            currentSnapshotId, previousSnapshotId, "file:/tmp/manifest2.avro");

    Snapshot currentSnapshot =
        new BaseSnapshot(
            0,
            currentSnapshotId,
            previousSnapshotId,
            currentSnapshotId,
            null,
            null,
            7,
            manifestList);

    List<HistoryEntry> snapshotLog =
        ImmutableList.<HistoryEntry>builder()
            .add(
                new SnapshotLogEntry(
                    previousSnapshot.timestampMillis(), previousSnapshot.snapshotId()))
            .add(
                new SnapshotLogEntry(
                    currentSnapshot.timestampMillis(), currentSnapshot.snapshotId()))
            .build();

    Schema schema = new Schema(6, Types.NestedField.required(10, "x", Types.StringType.get()));

    Map<String, SnapshotRef> refs =
        ImmutableMap.of("main", SnapshotRef.branchBuilder(previousSnapshotId).build());

    Assertions.assertThatThrownBy(
            () ->
                new TableMetadata(
                    null,
                    2,
                    UUID.randomUUID().toString(),
                    TEST_LOCATION,
                    SEQ_NO,
                    System.currentTimeMillis(),
                    3,
                    7,
                    ImmutableList.of(TEST_SCHEMA, schema),
                    5,
                    ImmutableList.of(SPEC_5),
                    SPEC_5.lastAssignedFieldId(),
                    3,
                    ImmutableList.of(SORT_ORDER_3),
                    ImmutableMap.of("property", "value"),
                    currentSnapshotId,
                    Arrays.asList(previousSnapshot, currentSnapshot),
                    null,
                    snapshotLog,
                    ImmutableList.of(),
                    refs,
                    ImmutableList.of(),
                    ImmutableList.of()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Current snapshot ID does not match main branch");
  }

  @Test
  public void testMainWithoutCurrent() throws IOException {
    long snapshotId = System.currentTimeMillis() - new Random(1234).nextInt(3600);

    String manifestList =
        createManifestListWithManifestFile(snapshotId, null, "file:/tmp/manifest1.avro");
    Snapshot snapshot =
        new BaseSnapshot(0, snapshotId, null, snapshotId, null, null, null, manifestList);

    Schema schema = new Schema(6, Types.NestedField.required(10, "x", Types.StringType.get()));

    Map<String, SnapshotRef> refs =
        ImmutableMap.of("main", SnapshotRef.branchBuilder(snapshotId).build());

    Assertions.assertThatThrownBy(
            () ->
                new TableMetadata(
                    null,
                    2,
                    UUID.randomUUID().toString(),
                    TEST_LOCATION,
                    SEQ_NO,
                    System.currentTimeMillis(),
                    3,
                    7,
                    ImmutableList.of(TEST_SCHEMA, schema),
                    5,
                    ImmutableList.of(SPEC_5),
                    SPEC_5.lastAssignedFieldId(),
                    3,
                    ImmutableList.of(SORT_ORDER_3),
                    ImmutableMap.of("property", "value"),
                    -1,
                    ImmutableList.of(snapshot),
                    null,
                    ImmutableList.of(),
                    ImmutableList.of(),
                    refs,
                    ImmutableList.of(),
                    ImmutableList.of()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Current snapshot is not set, but main branch exists");
  }

  @Test
  public void testBranchSnapshotMissing() {
    long snapshotId = System.currentTimeMillis() - new Random(1234).nextInt(3600);

    Schema schema = new Schema(6, Types.NestedField.required(10, "x", Types.StringType.get()));

    Map<String, SnapshotRef> refs =
        ImmutableMap.of("main", SnapshotRef.branchBuilder(snapshotId).build());

    Assertions.assertThatThrownBy(
            () ->
                new TableMetadata(
                    null,
                    2,
                    UUID.randomUUID().toString(),
                    TEST_LOCATION,
                    SEQ_NO,
                    System.currentTimeMillis(),
                    3,
                    7,
                    ImmutableList.of(TEST_SCHEMA, schema),
                    5,
                    ImmutableList.of(SPEC_5),
                    SPEC_5.lastAssignedFieldId(),
                    3,
                    ImmutableList.of(SORT_ORDER_3),
                    ImmutableMap.of("property", "value"),
                    -1,
                    ImmutableList.of(),
                    null,
                    ImmutableList.of(),
                    ImmutableList.of(),
                    refs,
                    ImmutableList.of(),
                    ImmutableList.of()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageEndingWith("does not exist in the existing snapshots list");
  }

  private static String toJsonWithoutSpecAndSchemaList(TableMetadata metadata) {
    StringWriter writer = new StringWriter();
    try {
      JsonGenerator generator = JsonUtil.factory().createGenerator(writer);

      generator.writeStartObject(); // start table metadata object

      generator.writeNumberField(FORMAT_VERSION, 1);
      generator.writeStringField(LOCATION, metadata.location());
      generator.writeNumberField(LAST_UPDATED_MILLIS, metadata.lastUpdatedMillis());
      generator.writeNumberField(LAST_COLUMN_ID, metadata.lastColumnId());

      // mimic an old writer by writing only schema and not the current ID or schema list
      generator.writeFieldName(SCHEMA);
      SchemaParser.toJson(metadata.schema().asStruct(), generator);

      // mimic an old writer by writing only partition-spec and not the default ID or spec list
      generator.writeFieldName(PARTITION_SPEC);
      PartitionSpecParser.toJsonFields(metadata.spec(), generator);

      JsonUtil.writeStringMap(PROPERTIES, metadata.properties(), generator);

      generator.writeNumberField(
          CURRENT_SNAPSHOT_ID,
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
      throw new UncheckedIOException(String.format("Failed to write json for: %s", metadata), e);
    }
    return writer.toString();
  }

  @Test
  public void testJsonWithPreviousMetadataLog() throws Exception {
    long previousSnapshotId = System.currentTimeMillis() - new Random(1234).nextInt(3600);

    String manifestList =
        createManifestListWithManifestFile(previousSnapshotId, null, "file:/tmp/manifest1.avro");
    Snapshot previousSnapshot =
        new BaseSnapshot(
            0, previousSnapshotId, null, previousSnapshotId, null, null, null, manifestList);

    long currentSnapshotId = System.currentTimeMillis();
    manifestList =
        createManifestListWithManifestFile(
            currentSnapshotId, previousSnapshotId, "file:/tmp/manifest2.avro");
    Snapshot currentSnapshot =
        new BaseSnapshot(
            0,
            currentSnapshotId,
            previousSnapshotId,
            currentSnapshotId,
            null,
            null,
            null,
            manifestList);

    List<HistoryEntry> reversedSnapshotLog = Lists.newArrayList();
    long currentTimestamp = System.currentTimeMillis();
    List<MetadataLogEntry> previousMetadataLog = Lists.newArrayList();
    previousMetadataLog.add(
        new MetadataLogEntry(
            currentTimestamp, "/tmp/000001-" + UUID.randomUUID().toString() + ".metadata.json"));

    TableMetadata base =
        new TableMetadata(
            null,
            1,
            UUID.randomUUID().toString(),
            TEST_LOCATION,
            0,
            System.currentTimeMillis(),
            3,
            7,
            ImmutableList.of(TEST_SCHEMA),
            5,
            ImmutableList.of(SPEC_5),
            SPEC_5.lastAssignedFieldId(),
            3,
            ImmutableList.of(SORT_ORDER_3),
            ImmutableMap.of("property", "value"),
            currentSnapshotId,
            Arrays.asList(previousSnapshot, currentSnapshot),
            null,
            reversedSnapshotLog,
            ImmutableList.copyOf(previousMetadataLog),
            ImmutableMap.of(),
            ImmutableList.of(),
            ImmutableList.of());

    String asJson = TableMetadataParser.toJson(base);
    TableMetadata metadataFromJson = TableMetadataParser.fromJson(asJson);

    Assert.assertEquals(
        "Metadata logs should match", previousMetadataLog, metadataFromJson.previousFiles());
  }

  @Test
  public void testAddPreviousMetadataRemoveNone() throws IOException {
    long previousSnapshotId = System.currentTimeMillis() - new Random(1234).nextInt(3600);

    String manifestList =
        createManifestListWithManifestFile(previousSnapshotId, null, "file:/tmp/manifest1.avro");
    Snapshot previousSnapshot =
        new BaseSnapshot(
            0, previousSnapshotId, null, previousSnapshotId, null, null, null, manifestList);
    long currentSnapshotId = System.currentTimeMillis();

    manifestList =
        createManifestListWithManifestFile(
            currentSnapshotId, previousSnapshotId, "file:/tmp/manifest2.avro");
    Snapshot currentSnapshot =
        new BaseSnapshot(
            0,
            currentSnapshotId,
            previousSnapshotId,
            currentSnapshotId,
            null,
            null,
            null,
            manifestList);

    List<HistoryEntry> reversedSnapshotLog = Lists.newArrayList();
    reversedSnapshotLog.add(
        new SnapshotLogEntry(previousSnapshot.timestampMillis(), previousSnapshotId));
    reversedSnapshotLog.add(
        new SnapshotLogEntry(currentSnapshot.timestampMillis(), currentSnapshotId));
    long currentTimestamp = System.currentTimeMillis();
    List<MetadataLogEntry> previousMetadataLog = Lists.newArrayList();
    previousMetadataLog.add(
        new MetadataLogEntry(
            currentTimestamp - 100,
            "/tmp/000001-" + UUID.randomUUID().toString() + ".metadata.json"));
    previousMetadataLog.add(
        new MetadataLogEntry(
            currentTimestamp - 90,
            "/tmp/000002-" + UUID.randomUUID().toString() + ".metadata.json"));

    MetadataLogEntry latestPreviousMetadata =
        new MetadataLogEntry(
            currentTimestamp - 80,
            "/tmp/000003-" + UUID.randomUUID().toString() + ".metadata.json");

    TableMetadata base =
        new TableMetadata(
            latestPreviousMetadata.file(),
            1,
            UUID.randomUUID().toString(),
            TEST_LOCATION,
            0,
            currentTimestamp - 80,
            3,
            7,
            ImmutableList.of(TEST_SCHEMA),
            5,
            ImmutableList.of(SPEC_5),
            SPEC_5.lastAssignedFieldId(),
            3,
            ImmutableList.of(SORT_ORDER_3),
            ImmutableMap.of("property", "value"),
            currentSnapshotId,
            Arrays.asList(previousSnapshot, currentSnapshot),
            null,
            reversedSnapshotLog,
            ImmutableList.copyOf(previousMetadataLog),
            ImmutableMap.of(),
            ImmutableList.of(),
            ImmutableList.of());

    previousMetadataLog.add(latestPreviousMetadata);

    TableMetadata metadata =
        base.replaceProperties(
            ImmutableMap.of(TableProperties.METADATA_PREVIOUS_VERSIONS_MAX, "5"));
    Set<MetadataLogEntry> removedPreviousMetadata = Sets.newHashSet(base.previousFiles());
    removedPreviousMetadata.removeAll(metadata.previousFiles());

    Assert.assertEquals(
        "Metadata logs should match", previousMetadataLog, metadata.previousFiles());
    Assert.assertEquals("Removed Metadata logs should be empty", 0, removedPreviousMetadata.size());
  }

  @Test
  public void testAddPreviousMetadataRemoveOne() throws IOException {
    long previousSnapshotId = System.currentTimeMillis() - new Random(1234).nextInt(3600);

    String manifestList =
        createManifestListWithManifestFile(previousSnapshotId, null, "file:/tmp/manifest1.avro");
    Snapshot previousSnapshot =
        new BaseSnapshot(
            0, previousSnapshotId, null, previousSnapshotId, null, null, null, manifestList);

    long currentSnapshotId = System.currentTimeMillis();
    manifestList =
        createManifestListWithManifestFile(
            currentSnapshotId, previousSnapshotId, "file:/tmp/manifest2.avro");
    Snapshot currentSnapshot =
        new BaseSnapshot(
            0,
            currentSnapshotId,
            previousSnapshotId,
            currentSnapshotId,
            null,
            null,
            null,
            manifestList);

    List<HistoryEntry> reversedSnapshotLog = Lists.newArrayList();
    reversedSnapshotLog.add(
        new SnapshotLogEntry(previousSnapshot.timestampMillis(), previousSnapshotId));
    reversedSnapshotLog.add(
        new SnapshotLogEntry(currentSnapshot.timestampMillis(), currentSnapshotId));
    long currentTimestamp = System.currentTimeMillis();
    List<MetadataLogEntry> previousMetadataLog = Lists.newArrayList();
    previousMetadataLog.add(
        new MetadataLogEntry(
            currentTimestamp - 100,
            "/tmp/000001-" + UUID.randomUUID().toString() + ".metadata.json"));
    previousMetadataLog.add(
        new MetadataLogEntry(
            currentTimestamp - 90,
            "/tmp/000002-" + UUID.randomUUID().toString() + ".metadata.json"));
    previousMetadataLog.add(
        new MetadataLogEntry(
            currentTimestamp - 80,
            "/tmp/000003-" + UUID.randomUUID().toString() + ".metadata.json"));
    previousMetadataLog.add(
        new MetadataLogEntry(
            currentTimestamp - 70,
            "/tmp/000004-" + UUID.randomUUID().toString() + ".metadata.json"));
    previousMetadataLog.add(
        new MetadataLogEntry(
            currentTimestamp - 60,
            "/tmp/000005-" + UUID.randomUUID().toString() + ".metadata.json"));

    MetadataLogEntry latestPreviousMetadata =
        new MetadataLogEntry(
            currentTimestamp - 50,
            "/tmp/000006-" + UUID.randomUUID().toString() + ".metadata.json");

    TableMetadata base =
        new TableMetadata(
            latestPreviousMetadata.file(),
            1,
            UUID.randomUUID().toString(),
            TEST_LOCATION,
            0,
            currentTimestamp - 50,
            3,
            7,
            ImmutableList.of(TEST_SCHEMA),
            5,
            ImmutableList.of(SPEC_5),
            SPEC_5.lastAssignedFieldId(),
            3,
            ImmutableList.of(SORT_ORDER_3),
            ImmutableMap.of("property", "value"),
            currentSnapshotId,
            Arrays.asList(previousSnapshot, currentSnapshot),
            null,
            reversedSnapshotLog,
            ImmutableList.copyOf(previousMetadataLog),
            ImmutableMap.of(),
            ImmutableList.of(),
            ImmutableList.of());

    previousMetadataLog.add(latestPreviousMetadata);

    TableMetadata metadata =
        base.replaceProperties(
            ImmutableMap.of(TableProperties.METADATA_PREVIOUS_VERSIONS_MAX, "5"));

    SortedSet<MetadataLogEntry> removedPreviousMetadata =
        Sets.newTreeSet(Comparator.comparingLong(MetadataLogEntry::timestampMillis));
    removedPreviousMetadata.addAll(base.previousFiles());
    removedPreviousMetadata.removeAll(metadata.previousFiles());

    Assert.assertEquals(
        "Metadata logs should match", previousMetadataLog.subList(1, 6), metadata.previousFiles());
    Assert.assertEquals(
        "Removed Metadata logs should contain 1",
        previousMetadataLog.subList(0, 1),
        ImmutableList.copyOf(removedPreviousMetadata));
  }

  @Test
  public void testAddPreviousMetadataRemoveMultiple() throws IOException {
    long previousSnapshotId = System.currentTimeMillis() - new Random(1234).nextInt(3600);

    String manifestList =
        createManifestListWithManifestFile(previousSnapshotId, null, "file:/tmp/manifest1.avro");
    Snapshot previousSnapshot =
        new BaseSnapshot(
            0, previousSnapshotId, null, previousSnapshotId, null, null, null, manifestList);

    long currentSnapshotId = System.currentTimeMillis();
    manifestList =
        createManifestListWithManifestFile(
            currentSnapshotId, previousSnapshotId, "file:/tmp/manifest2.avro");
    Snapshot currentSnapshot =
        new BaseSnapshot(
            0,
            currentSnapshotId,
            previousSnapshotId,
            currentSnapshotId,
            null,
            null,
            null,
            manifestList);

    List<HistoryEntry> reversedSnapshotLog = Lists.newArrayList();
    reversedSnapshotLog.add(
        new SnapshotLogEntry(previousSnapshot.timestampMillis(), previousSnapshotId));
    reversedSnapshotLog.add(
        new SnapshotLogEntry(currentSnapshot.timestampMillis(), currentSnapshotId));
    long currentTimestamp = System.currentTimeMillis();
    List<MetadataLogEntry> previousMetadataLog = Lists.newArrayList();
    previousMetadataLog.add(
        new MetadataLogEntry(
            currentTimestamp - 100,
            "/tmp/000001-" + UUID.randomUUID().toString() + ".metadata.json"));
    previousMetadataLog.add(
        new MetadataLogEntry(
            currentTimestamp - 90,
            "/tmp/000002-" + UUID.randomUUID().toString() + ".metadata.json"));
    previousMetadataLog.add(
        new MetadataLogEntry(
            currentTimestamp - 80,
            "/tmp/000003-" + UUID.randomUUID().toString() + ".metadata.json"));
    previousMetadataLog.add(
        new MetadataLogEntry(
            currentTimestamp - 70,
            "/tmp/000004-" + UUID.randomUUID().toString() + ".metadata.json"));
    previousMetadataLog.add(
        new MetadataLogEntry(
            currentTimestamp - 60,
            "/tmp/000005-" + UUID.randomUUID().toString() + ".metadata.json"));

    MetadataLogEntry latestPreviousMetadata =
        new MetadataLogEntry(
            currentTimestamp - 50,
            "/tmp/000006-" + UUID.randomUUID().toString() + ".metadata.json");

    TableMetadata base =
        new TableMetadata(
            latestPreviousMetadata.file(),
            1,
            UUID.randomUUID().toString(),
            TEST_LOCATION,
            0,
            currentTimestamp - 50,
            3,
            7,
            ImmutableList.of(TEST_SCHEMA),
            SPEC_5.specId(),
            ImmutableList.of(SPEC_5),
            SPEC_5.lastAssignedFieldId(),
            SortOrder.unsorted().orderId(),
            ImmutableList.of(SortOrder.unsorted()),
            ImmutableMap.of("property", "value"),
            currentSnapshotId,
            Arrays.asList(previousSnapshot, currentSnapshot),
            null,
            reversedSnapshotLog,
            ImmutableList.copyOf(previousMetadataLog),
            ImmutableMap.of(),
            ImmutableList.of(),
            ImmutableList.of());

    previousMetadataLog.add(latestPreviousMetadata);

    TableMetadata metadata =
        base.replaceProperties(
            ImmutableMap.of(TableProperties.METADATA_PREVIOUS_VERSIONS_MAX, "2"));

    SortedSet<MetadataLogEntry> removedPreviousMetadata =
        Sets.newTreeSet(Comparator.comparingLong(MetadataLogEntry::timestampMillis));
    removedPreviousMetadata.addAll(base.previousFiles());
    removedPreviousMetadata.removeAll(metadata.previousFiles());

    Assert.assertEquals(
        "Metadata logs should match", previousMetadataLog.subList(4, 6), metadata.previousFiles());
    Assert.assertEquals(
        "Removed Metadata logs should contain 4",
        previousMetadataLog.subList(0, 4),
        ImmutableList.copyOf(removedPreviousMetadata));
  }

  @Test
  public void testV2UUIDValidation() {
    Assertions.assertThatThrownBy(
            () ->
                new TableMetadata(
                    null,
                    2,
                    null,
                    TEST_LOCATION,
                    SEQ_NO,
                    System.currentTimeMillis(),
                    LAST_ASSIGNED_COLUMN_ID,
                    7,
                    ImmutableList.of(TEST_SCHEMA),
                    SPEC_5.specId(),
                    ImmutableList.of(SPEC_5),
                    SPEC_5.lastAssignedFieldId(),
                    3,
                    ImmutableList.of(SORT_ORDER_3),
                    ImmutableMap.of(),
                    -1L,
                    ImmutableList.of(),
                    null,
                    ImmutableList.of(),
                    ImmutableList.of(),
                    ImmutableMap.of(),
                    ImmutableList.of(),
                    ImmutableList.of()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("UUID is required in format v2");
  }

  @Test
  public void testVersionValidation() {
    int unsupportedVersion = TableMetadata.SUPPORTED_TABLE_FORMAT_VERSION + 1;
    Assertions.assertThatThrownBy(
            () ->
                new TableMetadata(
                    null,
                    unsupportedVersion,
                    null,
                    TEST_LOCATION,
                    SEQ_NO,
                    System.currentTimeMillis(),
                    LAST_ASSIGNED_COLUMN_ID,
                    7,
                    ImmutableList.of(TEST_SCHEMA),
                    SPEC_5.specId(),
                    ImmutableList.of(SPEC_5),
                    SPEC_5.lastAssignedFieldId(),
                    3,
                    ImmutableList.of(SORT_ORDER_3),
                    ImmutableMap.of(),
                    -1L,
                    ImmutableList.of(),
                    null,
                    ImmutableList.of(),
                    ImmutableList.of(),
                    ImmutableMap.of(),
                    ImmutableList.of(),
                    ImmutableList.of()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Unsupported format version: v" + unsupportedVersion);
  }

  @Test
  public void testParserVersionValidation() throws Exception {
    String supportedVersion1 = readTableMetadataInputFile("TableMetadataV1Valid.json");
    TableMetadata parsed1 = TableMetadataParser.fromJson(supportedVersion1);
    Assert.assertNotNull("Should successfully read supported metadata version", parsed1);

    String supportedVersion2 = readTableMetadataInputFile("TableMetadataV2Valid.json");
    TableMetadata parsed2 = TableMetadataParser.fromJson(supportedVersion2);
    Assert.assertNotNull("Should successfully read supported metadata version", parsed2);

    String unsupportedVersion = readTableMetadataInputFile("TableMetadataUnsupportedVersion.json");
    Assertions.assertThatThrownBy(() -> TableMetadataParser.fromJson(unsupportedVersion))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Cannot read unsupported version");
  }

  @Test
  public void testParserV2PartitionSpecsValidation() throws Exception {
    String unsupportedVersion =
        readTableMetadataInputFile("TableMetadataV2MissingPartitionSpecs.json");
    Assertions.assertThatThrownBy(() -> TableMetadataParser.fromJson(unsupportedVersion))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("partition-specs must exist in format v2");
  }

  @Test
  public void testParserV2LastAssignedFieldIdValidation() throws Exception {
    String unsupportedVersion =
        readTableMetadataInputFile("TableMetadataV2MissingLastPartitionId.json");
    Assertions.assertThatThrownBy(() -> TableMetadataParser.fromJson(unsupportedVersion))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("last-partition-id must exist in format v2");
  }

  @Test
  public void testParserV2SortOrderValidation() throws Exception {
    String unsupportedVersion = readTableMetadataInputFile("TableMetadataV2MissingSortOrder.json");
    Assertions.assertThatThrownBy(() -> TableMetadataParser.fromJson(unsupportedVersion))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("sort-orders must exist in format v2");
  }

  @Test
  public void testParserV2CurrentSchemaIdValidation() throws Exception {
    String unsupported = readTableMetadataInputFile("TableMetadataV2CurrentSchemaNotFound.json");
    Assertions.assertThatThrownBy(() -> TableMetadataParser.fromJson(unsupported))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot find schema with current-schema-id=2 from schemas");
  }

  @Test
  public void testParserV2SchemasValidation() throws Exception {
    String unsupported = readTableMetadataInputFile("TableMetadataV2MissingSchemas.json");
    Assertions.assertThatThrownBy(() -> TableMetadataParser.fromJson(unsupported))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("schemas must exist in format v2");
  }

  private String readTableMetadataInputFile(String fileName) throws Exception {
    Path path = Paths.get(getClass().getClassLoader().getResource(fileName).toURI());
    return String.join("", java.nio.file.Files.readAllLines(path));
  }

  @Test
  public void testNewTableMetadataReassignmentAllIds() throws Exception {
    Schema schema =
        new Schema(
            Types.NestedField.required(3, "x", Types.LongType.get()),
            Types.NestedField.required(4, "y", Types.LongType.get()),
            Types.NestedField.required(5, "z", Types.LongType.get()));

    PartitionSpec spec =
        PartitionSpec.builderFor(schema)
            .withSpecId(5)
            .add(3, 1005, "x_partition", Transforms.bucket(4))
            .add(5, 1003, "z_partition", Transforms.bucket(8))
            .build();
    String location = "file://tmp/db/table";
    TableMetadata metadata =
        TableMetadata.newTableMetadata(schema, spec, location, ImmutableMap.of());

    // newTableMetadata should reassign column ids and partition field ids.
    PartitionSpec expected =
        PartitionSpec.builderFor(metadata.schema())
            .withSpecId(0)
            .add(1, 1000, "x_partition", Transforms.bucket(4))
            .add(3, 1001, "z_partition", Transforms.bucket(8))
            .build();

    Assert.assertEquals(expected, metadata.spec());
  }

  @Test
  public void testInvalidUpdatePartitionSpecForV1Table() throws Exception {
    Schema schema = new Schema(Types.NestedField.required(1, "x", Types.LongType.get()));

    PartitionSpec spec =
        PartitionSpec.builderFor(schema)
            .withSpecId(5)
            .add(1, 1005, "x_partition", Transforms.bucket(4))
            .build();
    String location = "file://tmp/db/table";
    TableMetadata metadata =
        TableMetadata.newTableMetadata(
            schema, PartitionSpec.unpartitioned(), location, ImmutableMap.of());

    Assertions.assertThatThrownBy(() -> metadata.updatePartitionSpec(spec))
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith("Spec does not use sequential IDs that are required in v1");
  }

  @Test
  public void testBuildReplacementForV1Table() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "x", Types.LongType.get()),
            Types.NestedField.required(2, "y", Types.LongType.get()));
    PartitionSpec spec =
        PartitionSpec.builderFor(schema).withSpecId(0).identity("x").identity("y").build();
    String location = "file://tmp/db/table";
    TableMetadata metadata =
        TableMetadata.newTableMetadata(
            schema, spec, SortOrder.unsorted(), location, ImmutableMap.of(), 1);
    Assert.assertEquals(spec, metadata.spec());

    Schema updatedSchema =
        new Schema(
            Types.NestedField.required(1, "x", Types.LongType.get()),
            Types.NestedField.required(2, "z", Types.StringType.get()),
            Types.NestedField.required(3, "y", Types.LongType.get()));
    PartitionSpec updatedSpec =
        PartitionSpec.builderFor(updatedSchema).withSpecId(0).bucket("z", 8).identity("x").build();
    TableMetadata updated =
        metadata.buildReplacement(
            updatedSchema, updatedSpec, SortOrder.unsorted(), location, ImmutableMap.of());
    PartitionSpec expected =
        PartitionSpec.builderFor(updated.schema())
            .withSpecId(1)
            .add(1, 1000, "x", Transforms.identity())
            .add(2, 1001, "y", Transforms.alwaysNull())
            .add(3, 1002, "z_bucket", Transforms.bucket(8))
            .build();
    Assert.assertEquals(
        "Should reassign the partition field IDs and reuse any existing IDs for equivalent fields",
        expected,
        updated.spec());
  }

  @Test
  public void testBuildReplacementForV2Table() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "x", Types.LongType.get()),
            Types.NestedField.required(2, "y", Types.LongType.get()));
    PartitionSpec spec =
        PartitionSpec.builderFor(schema).withSpecId(0).identity("x").identity("y").build();
    String location = "file://tmp/db/table";
    TableMetadata metadata =
        TableMetadata.newTableMetadata(
            schema, spec, SortOrder.unsorted(), location, ImmutableMap.of(), 2);
    Assert.assertEquals(spec, metadata.spec());

    Schema updatedSchema =
        new Schema(
            Types.NestedField.required(1, "x", Types.LongType.get()),
            Types.NestedField.required(2, "z", Types.StringType.get()));
    PartitionSpec updatedSpec =
        PartitionSpec.builderFor(updatedSchema).withSpecId(0).bucket("z", 8).identity("x").build();
    TableMetadata updated =
        metadata.buildReplacement(
            updatedSchema, updatedSpec, SortOrder.unsorted(), location, ImmutableMap.of());
    PartitionSpec expected =
        PartitionSpec.builderFor(updated.schema())
            .withSpecId(1)
            .add(3, 1002, "z_bucket", Transforms.bucket(8))
            .add(1, 1000, "x", Transforms.identity())
            .build();
    Assert.assertEquals(
        "Should reassign the partition field IDs and reuse any existing IDs for equivalent fields",
        expected,
        updated.spec());
  }

  @Test
  public void testSortOrder() {
    Schema schema = new Schema(Types.NestedField.required(10, "x", Types.StringType.get()));

    TableMetadata meta =
        TableMetadata.newTableMetadata(
            schema, PartitionSpec.unpartitioned(), null, ImmutableMap.of());
    Assert.assertTrue("Should default to unsorted order", meta.sortOrder().isUnsorted());
    Assert.assertSame(
        "Should detect identical unsorted order",
        meta,
        meta.replaceSortOrder(SortOrder.unsorted()));
  }

  @Test
  public void testUpdateSortOrder() {
    Schema schema = new Schema(Types.NestedField.required(10, "x", Types.StringType.get()));

    SortOrder order = SortOrder.builderFor(schema).asc("x").build();

    TableMetadata sortedByX =
        TableMetadata.newTableMetadata(
            schema, PartitionSpec.unpartitioned(), order, null, ImmutableMap.of());
    Assert.assertEquals("Should have 1 sort order", 1, sortedByX.sortOrders().size());
    Assert.assertEquals("Should use orderId 1", 1, sortedByX.sortOrder().orderId());
    Assert.assertEquals("Should be sorted by one field", 1, sortedByX.sortOrder().fields().size());
    Assert.assertEquals(
        "Should use the table's field ids", 1, sortedByX.sortOrder().fields().get(0).sourceId());
    Assert.assertEquals(
        "Should be ascending",
        SortDirection.ASC,
        sortedByX.sortOrder().fields().get(0).direction());
    Assert.assertEquals(
        "Should be nulls first",
        NullOrder.NULLS_FIRST,
        sortedByX.sortOrder().fields().get(0).nullOrder());

    // build an equivalent order with the correct schema
    SortOrder newOrder = SortOrder.builderFor(sortedByX.schema()).asc("x").build();

    TableMetadata alsoSortedByX = sortedByX.replaceSortOrder(newOrder);
    Assert.assertSame("Should detect current sortOrder and not update", alsoSortedByX, sortedByX);

    TableMetadata unsorted = alsoSortedByX.replaceSortOrder(SortOrder.unsorted());
    Assert.assertEquals("Should have 2 sort orders", 2, unsorted.sortOrders().size());
    Assert.assertEquals("Should use orderId 0", 0, unsorted.sortOrder().orderId());
    Assert.assertTrue("Should be unsorted", unsorted.sortOrder().isUnsorted());

    TableMetadata sortedByXDesc =
        unsorted.replaceSortOrder(SortOrder.builderFor(unsorted.schema()).desc("x").build());
    Assert.assertEquals("Should have 3 sort orders", 3, sortedByXDesc.sortOrders().size());
    Assert.assertEquals("Should use orderId 2", 2, sortedByXDesc.sortOrder().orderId());
    Assert.assertEquals(
        "Should be sorted by one field", 1, sortedByXDesc.sortOrder().fields().size());
    Assert.assertEquals(
        "Should use the table's field ids",
        1,
        sortedByXDesc.sortOrder().fields().get(0).sourceId());
    Assert.assertEquals(
        "Should be ascending",
        SortDirection.DESC,
        sortedByXDesc.sortOrder().fields().get(0).direction());
    Assert.assertEquals(
        "Should be nulls first",
        NullOrder.NULLS_FIRST,
        sortedByX.sortOrder().fields().get(0).nullOrder());
  }

  @Test
  public void testStatistics() {
    Schema schema = new Schema(Types.NestedField.required(10, "x", Types.StringType.get()));

    TableMetadata meta =
        TableMetadata.newTableMetadata(
            schema, PartitionSpec.unpartitioned(), null, ImmutableMap.of());
    Assert.assertEquals(
        "Should default to no statistics files", ImmutableList.of(), meta.statisticsFiles());
  }

  @Test
  public void testSetStatistics() {
    Schema schema = new Schema(Types.NestedField.required(10, "x", Types.StringType.get()));

    TableMetadata meta =
        TableMetadata.newTableMetadata(
            schema, PartitionSpec.unpartitioned(), null, ImmutableMap.of());

    TableMetadata withStatistics =
        TableMetadata.buildFrom(meta)
            .setStatistics(
                43,
                new GenericStatisticsFile(
                    43, "/some/path/to/stats/file", 128, 27, ImmutableList.of()))
            .build();

    Assertions.assertThat(withStatistics.statisticsFiles())
        .as("There should be one statistics file registered")
        .hasSize(1);
    StatisticsFile statisticsFile = Iterables.getOnlyElement(withStatistics.statisticsFiles());
    Assert.assertEquals("Statistics file snapshot", 43L, statisticsFile.snapshotId());
    Assert.assertEquals("Statistics file path", "/some/path/to/stats/file", statisticsFile.path());

    TableMetadata withStatisticsReplaced =
        TableMetadata.buildFrom(withStatistics)
            .setStatistics(
                43,
                new GenericStatisticsFile(
                    43, "/some/path/to/stats/file2", 128, 27, ImmutableList.of()))
            .build();

    Assertions.assertThat(withStatisticsReplaced.statisticsFiles())
        .as("There should be one statistics file registered")
        .hasSize(1);
    statisticsFile = Iterables.getOnlyElement(withStatisticsReplaced.statisticsFiles());
    Assert.assertEquals("Statistics file snapshot", 43L, statisticsFile.snapshotId());
    Assert.assertEquals("Statistics file path", "/some/path/to/stats/file2", statisticsFile.path());
  }

  @Test
  public void testRemoveStatistics() {
    Schema schema = new Schema(Types.NestedField.required(10, "x", Types.StringType.get()));

    TableMetadata meta =
        TableMetadata.buildFrom(
                TableMetadata.newTableMetadata(
                    schema, PartitionSpec.unpartitioned(), null, ImmutableMap.of()))
            .setStatistics(
                43,
                new GenericStatisticsFile(
                    43, "/some/path/to/stats/file", 128, 27, ImmutableList.of()))
            .setStatistics(
                44,
                new GenericStatisticsFile(
                    44, "/some/path/to/stats/file2", 128, 27, ImmutableList.of()))
            .build();

    Assert.assertSame(
        "Should detect no statistics to remove",
        meta,
        TableMetadata.buildFrom(meta).removeStatistics(42L).build());

    TableMetadata withOneRemoved = TableMetadata.buildFrom(meta).removeStatistics(43).build();

    Assertions.assertThat(withOneRemoved.statisticsFiles())
        .as("There should be one statistics file retained")
        .hasSize(1);
    StatisticsFile statisticsFile = Iterables.getOnlyElement(withOneRemoved.statisticsFiles());
    Assert.assertEquals("Statistics file snapshot", 44L, statisticsFile.snapshotId());
    Assert.assertEquals("Statistics file path", "/some/path/to/stats/file2", statisticsFile.path());
  }

  @Test
  public void testParseSchemaIdentifierFields() throws Exception {
    String data = readTableMetadataInputFile("TableMetadataV2Valid.json");
    TableMetadata parsed = TableMetadataParser.fromJson(data);
    Assert.assertEquals(Sets.newHashSet(), parsed.schemasById().get(0).identifierFieldIds());
    Assert.assertEquals(Sets.newHashSet(1, 2), parsed.schemasById().get(1).identifierFieldIds());
  }

  @Test
  public void testUpdateSchemaIdentifierFields() {
    Schema schema = new Schema(Types.NestedField.required(10, "x", Types.StringType.get()));

    TableMetadata meta =
        TableMetadata.newTableMetadata(
            schema, PartitionSpec.unpartitioned(), null, ImmutableMap.of());

    Schema newSchema =
        new Schema(
            Lists.newArrayList(Types.NestedField.required(1, "x", Types.StringType.get())),
            Sets.newHashSet(1));
    TableMetadata newMeta = meta.updateSchema(newSchema, 1);
    Assert.assertEquals(2, newMeta.schemas().size());
    Assert.assertEquals(Sets.newHashSet(1), newMeta.schema().identifierFieldIds());
  }

  @Test
  public void testUpdateSchema() {
    Schema schema =
        new Schema(0, Types.NestedField.required(1, "y", Types.LongType.get(), "comment"));
    TableMetadata freshTable =
        TableMetadata.newTableMetadata(
            schema, PartitionSpec.unpartitioned(), null, ImmutableMap.of());
    Assert.assertEquals(
        "Should use TableMetadata.INITIAL_SCHEMA_ID for current schema id",
        TableMetadata.INITIAL_SCHEMA_ID,
        freshTable.currentSchemaId());
    assertSameSchemaList(ImmutableList.of(schema), freshTable.schemas());
    Assert.assertEquals(
        "Should have expected schema upon return",
        schema.asStruct(),
        freshTable.schema().asStruct());
    Assert.assertEquals("Should return expected last column id", 1, freshTable.lastColumnId());

    // update schema
    Schema schema2 =
        new Schema(
            Types.NestedField.required(1, "y", Types.LongType.get(), "comment"),
            Types.NestedField.required(2, "x", Types.StringType.get()));
    TableMetadata twoSchemasTable = freshTable.updateSchema(schema2, 2);
    Assert.assertEquals("Should have current schema id as 1", 1, twoSchemasTable.currentSchemaId());
    assertSameSchemaList(
        ImmutableList.of(schema, new Schema(1, schema2.columns())), twoSchemasTable.schemas());
    Assert.assertEquals(
        "Should have expected schema upon return",
        schema2.asStruct(),
        twoSchemasTable.schema().asStruct());
    Assert.assertEquals("Should return expected last column id", 2, twoSchemasTable.lastColumnId());

    // update schema with the same schema and last column ID as current shouldn't cause change
    Schema sameSchema2 =
        new Schema(
            Types.NestedField.required(1, "y", Types.LongType.get(), "comment"),
            Types.NestedField.required(2, "x", Types.StringType.get()));
    TableMetadata sameSchemaTable = twoSchemasTable.updateSchema(sameSchema2, 2);
    Assert.assertSame("Should return same table metadata", twoSchemasTable, sameSchemaTable);

    // update schema with the same schema and different last column ID as current should create
    // a new table
    TableMetadata differentColumnIdTable = sameSchemaTable.updateSchema(sameSchema2, 3);
    Assert.assertEquals(
        "Should have current schema id as 1", 1, differentColumnIdTable.currentSchemaId());
    assertSameSchemaList(
        ImmutableList.of(schema, new Schema(1, schema2.columns())),
        differentColumnIdTable.schemas());
    Assert.assertEquals(
        "Should have expected schema upon return",
        schema2.asStruct(),
        differentColumnIdTable.schema().asStruct());
    Assert.assertEquals(
        "Should return expected last column id", 3, differentColumnIdTable.lastColumnId());

    // update schema with old schema does not change schemas
    TableMetadata revertSchemaTable = differentColumnIdTable.updateSchema(schema, 3);
    Assert.assertEquals(
        "Should have current schema id as 0", 0, revertSchemaTable.currentSchemaId());
    assertSameSchemaList(
        ImmutableList.of(schema, new Schema(1, schema2.columns())), revertSchemaTable.schemas());
    Assert.assertEquals(
        "Should have expected schema upon return",
        schema.asStruct(),
        revertSchemaTable.schema().asStruct());
    Assert.assertEquals(
        "Should return expected last column id", 3, revertSchemaTable.lastColumnId());

    // create new schema will use the largest schema id + 1
    Schema schema3 =
        new Schema(
            Types.NestedField.required(2, "y", Types.LongType.get(), "comment"),
            Types.NestedField.required(4, "x", Types.StringType.get()),
            Types.NestedField.required(6, "z", Types.IntegerType.get()));
    TableMetadata threeSchemaTable = revertSchemaTable.updateSchema(schema3, 6);
    Assert.assertEquals(
        "Should have current schema id as 2", 2, threeSchemaTable.currentSchemaId());
    assertSameSchemaList(
        ImmutableList.of(
            schema, new Schema(1, schema2.columns()), new Schema(2, schema3.columns())),
        threeSchemaTable.schemas());
    Assert.assertEquals(
        "Should have expected schema upon return",
        schema3.asStruct(),
        threeSchemaTable.schema().asStruct());
    Assert.assertEquals(
        "Should return expected last column id", 6, threeSchemaTable.lastColumnId());
  }

  @Test
  public void testCreateV2MetadataThroughTableProperty() {
    Schema schema = new Schema(Types.NestedField.required(10, "x", Types.StringType.get()));

    TableMetadata meta =
        TableMetadata.newTableMetadata(
            schema,
            PartitionSpec.unpartitioned(),
            null,
            ImmutableMap.of(TableProperties.FORMAT_VERSION, "2", "key", "val"));

    Assert.assertEquals(
        "format version should be configured based on the format-version key",
        2,
        meta.formatVersion());
    Assert.assertEquals(
        "should not contain format-version in properties",
        ImmutableMap.of("key", "val"),
        meta.properties());
  }

  @Test
  public void testReplaceV1MetadataToV2ThroughTableProperty() {
    Schema schema = new Schema(Types.NestedField.required(10, "x", Types.StringType.get()));

    TableMetadata meta =
        TableMetadata.newTableMetadata(
            schema,
            PartitionSpec.unpartitioned(),
            null,
            ImmutableMap.of(TableProperties.FORMAT_VERSION, "1", "key", "val"));

    meta =
        meta.buildReplacement(
            meta.schema(),
            meta.spec(),
            meta.sortOrder(),
            meta.location(),
            ImmutableMap.of(TableProperties.FORMAT_VERSION, "2", "key2", "val2"));

    Assert.assertEquals(
        "format version should be configured based on the format-version key",
        2,
        meta.formatVersion());
    Assert.assertEquals(
        "should not contain format-version but should contain old and new properties",
        ImmutableMap.of("key", "val", "key2", "val2"),
        meta.properties());
  }

  @Test
  public void testUpgradeV1MetadataToV2ThroughTableProperty() {
    Schema schema = new Schema(Types.NestedField.required(10, "x", Types.StringType.get()));

    TableMetadata meta =
        TableMetadata.newTableMetadata(
            schema,
            PartitionSpec.unpartitioned(),
            null,
            ImmutableMap.of(TableProperties.FORMAT_VERSION, "1", "key", "val"));

    meta =
        meta.replaceProperties(
            ImmutableMap.of(TableProperties.FORMAT_VERSION, "2", "key2", "val2"));

    Assert.assertEquals(
        "format version should be configured based on the format-version key",
        2,
        meta.formatVersion());
    Assert.assertEquals(
        "should not contain format-version but should contain new properties",
        ImmutableMap.of("key2", "val2"),
        meta.properties());
  }

  @Test
  public void testParseStatisticsFiles() throws Exception {
    String data = readTableMetadataInputFile("TableMetadataStatisticsFiles.json");
    TableMetadata parsed = TableMetadataParser.fromJson(data);
    Assertions.assertThat(parsed.statisticsFiles()).as("parsed statistics files").hasSize(1);
    Assert.assertEquals(
        "parsed statistics file",
        new GenericStatisticsFile(
            3055729675574597004L,
            "s3://a/b/stats.puffin",
            413,
            42,
            ImmutableList.of(
                new GenericBlobMetadata(
                    "ndv", 3055729675574597004L, 1, ImmutableList.of(1), ImmutableMap.of()))),
        Iterables.getOnlyElement(parsed.statisticsFiles()));
  }

  @Test
  public void testNoReservedPropertyForTableMetadataCreation() {
    Schema schema = new Schema(Types.NestedField.required(10, "x", Types.StringType.get()));

    Assertions.assertThatThrownBy(
            () ->
                TableMetadata.newTableMetadata(
                    schema,
                    PartitionSpec.unpartitioned(),
                    null,
                    "/tmp",
                    ImmutableMap.of(TableProperties.FORMAT_VERSION, "1"),
                    1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Table properties should not contain reserved properties, but got {format-version=1}");

    Assertions.assertThatThrownBy(
            () ->
                TableMetadata.newTableMetadata(
                    schema,
                    PartitionSpec.unpartitioned(),
                    null,
                    "/tmp",
                    ImmutableMap.of(TableProperties.UUID, "uuid"),
                    1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Table properties should not contain reserved properties, but got {uuid=uuid}");
  }

  @Test
  public void testNoTrailingLocationSlash() {
    String locationWithSlash = "/with_trailing_slash/";
    String locationWithoutSlash = "/with_trailing_slash";
    TableMetadata meta =
        TableMetadata.newTableMetadata(
            TEST_SCHEMA, SPEC_5, SORT_ORDER_3, locationWithSlash, Collections.emptyMap());
    Assert.assertEquals(
        "Metadata should never return a location ending in a slash",
        locationWithoutSlash,
        meta.location());
  }

  private String createManifestListWithManifestFile(
      long snapshotId, Long parentSnapshotId, String manifestFile) throws IOException {
    File manifestList = temp.newFile("manifests" + UUID.randomUUID());
    manifestList.deleteOnExit();

    try (ManifestListWriter writer =
        ManifestLists.write(1, Files.localOutput(manifestList), snapshotId, parentSnapshotId, 0)) {
      writer.addAll(
          ImmutableList.of(new GenericManifestFile(localInput(manifestFile), SPEC_5.specId())));
    }

    return localInput(manifestList).location();
  }
}
