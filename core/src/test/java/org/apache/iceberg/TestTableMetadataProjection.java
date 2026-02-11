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

import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestTableMetadataProjection {

  private static final Schema TEST_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "x", Types.LongType.get()),
          Types.NestedField.required(2, "y", Types.LongType.get()),
          Types.NestedField.required(3, "z", Types.LongType.get()));

  private static final PartitionSpec SPEC_5 =
      PartitionSpec.builderFor(TEST_SCHEMA).withSpecId(5).build();

  private static final SortOrder SORT_ORDER_3 =
      SortOrder.builderFor(TEST_SCHEMA).withOrderId(3).asc("y", NullOrder.NULLS_FIRST).build();

  private static final String TEST_LOCATION = "s3://bucket/test/location";
  private static final int LAST_ASSIGNED_COLUMN_ID = 3;

  @Test
  public void testTransformSnapshotsWithoutLazyLoading() {
    long snapshotId1 = System.currentTimeMillis();
    Map<String, String> summary1 =
        ImmutableMap.of(
            "total-records", "100",
            "total-files", "10",
            "operation", "append");

    Snapshot snapshot1 =
        new BaseSnapshot(
            0,
            snapshotId1,
            null,
            snapshotId1,
            "append",
            summary1,
            TEST_SCHEMA.schemaId(),
            "file:/tmp/manifest1.avro",
            null,
            null,
            null);

    long snapshotId2 = snapshotId1 + 1;
    Map<String, String> summary2 =
        ImmutableMap.of(
            "total-records", "200",
            "total-files", "20",
            "operation", "overwrite");

    Snapshot snapshot2 =
        new BaseSnapshot(
            1,
            snapshotId2,
            snapshotId1,
            snapshotId2,
            "overwrite",
            summary2,
            TEST_SCHEMA.schemaId(),
            "file:/tmp/manifest2.avro",
            null,
            null,
            null);

    TableMetadata base =
        new TableMetadata(
            null,
            2,
            UUID.randomUUID().toString(),
            TEST_LOCATION,
            1,
            System.currentTimeMillis(),
            LAST_ASSIGNED_COLUMN_ID,
            TEST_SCHEMA.schemaId(),
            ImmutableList.of(TEST_SCHEMA),
            SPEC_5.specId(),
            ImmutableList.of(SPEC_5),
            SPEC_5.lastAssignedFieldId(),
            SORT_ORDER_3.orderId(),
            ImmutableList.of(SORT_ORDER_3),
            ImmutableMap.of(),
            snapshotId2,
            ImmutableList.of(snapshot1, snapshot2),
            null,
            ImmutableList.of(),
            ImmutableList.of(),
            ImmutableMap.of(
                SnapshotRef.MAIN_BRANCH, SnapshotRef.branchBuilder(snapshotId2).build()),
            ImmutableList.of(),
            ImmutableList.of(),
            0L,
            ImmutableList.of(),
            ImmutableList.of());

    assertThat(base.snapshots()).hasSize(2);
    assertThat(base.snapshots().get(0).summary()).isEqualTo(summary1);
    assertThat(base.snapshots().get(1).summary()).isEqualTo(summary2);

    // Create projection that clears snapshot summaries
    TableMetadata projected =
        TableMetadataProjection.create(
            base,
            snapshot -> {
              if (snapshot instanceof BaseSnapshot) {
                BaseSnapshot tSnapshot = (BaseSnapshot) snapshot;
                return new BaseSnapshot(
                    tSnapshot.sequenceNumber(),
                    tSnapshot.snapshotId(),
                    tSnapshot.parentId(),
                    tSnapshot.timestampMillis(),
                    tSnapshot.operation(),
                    ImmutableMap.of(), // Clear summary
                    tSnapshot.schemaId(),
                    tSnapshot.manifestListLocation(),
                    tSnapshot.firstRowId(),
                    tSnapshot.addedRows(),
                    tSnapshot.keyId());
              }
              return snapshot;
            });

    // Verify transformed snapshots
    assertThat(projected.snapshots()).hasSize(2);
    assertThat(projected.snapshots().get(0).summary()).isEmpty();
    assertThat(projected.snapshots().get(1).summary()).isEmpty();
    assertThat(projected.snapshots().get(0).snapshotId()).isEqualTo(snapshotId1);
    assertThat(projected.snapshots().get(1).snapshotId()).isEqualTo(snapshotId2);
    assertThat(projected.snapshots().get(0).operation()).isEqualTo("append");
    assertThat(projected.snapshots().get(1).operation()).isEqualTo("overwrite");

    // Verify snapshot lookup by ID works
    assertThat(projected.snapshot(snapshotId1)).isNotNull();
    assertThat(projected.snapshot(snapshotId1).summary()).isEmpty();
    assertThat(projected.snapshot(snapshotId2)).isNotNull();
    assertThat(projected.snapshot(snapshotId2).summary()).isEmpty();

    // Verify current snapshot
    assertThat(projected.currentSnapshot()).isNotNull();
    assertThat(projected.currentSnapshot().snapshotId()).isEqualTo(snapshotId2);
    assertThat(projected.currentSnapshot().summary()).isEmpty();

    // Verify refs still work
    assertThat(projected.refs()).hasSize(1);
    assertThat(projected.ref(SnapshotRef.MAIN_BRANCH)).isNotNull();
    assertThat(projected.ref(SnapshotRef.MAIN_BRANCH).snapshotId()).isEqualTo(snapshotId2);
  }

  @Test
  public void testTransformSnapshotsWithLazyLoading() {
    long snapshotId1 = System.currentTimeMillis();
    Map<String, String> summary1 =
        ImmutableMap.of(
            "total-records", "100",
            "total-files", "10",
            "operation", "append");

    Snapshot snapshot1 =
        new BaseSnapshot(
            0,
            snapshotId1,
            null,
            snapshotId1,
            "append",
            summary1,
            TEST_SCHEMA.schemaId(),
            "file:/tmp/manifest1.avro",
            null,
            null,
            null);

    long snapshotId2 = snapshotId1 + 1;
    Map<String, String> summary2 =
        ImmutableMap.of(
            "total-records", "200",
            "total-files", "20",
            "operation", "overwrite");

    Snapshot snapshot2 =
        new BaseSnapshot(
            1,
            snapshotId2,
            snapshotId1,
            snapshotId2,
            "overwrite",
            summary2,
            TEST_SCHEMA.schemaId(),
            "file:/tmp/manifest2.avro",
            null,
            null,
            null);

    // Create TableMetadata with lazy snapshot loading
    TableMetadata base =
        new TableMetadata(
            null,
            2,
            UUID.randomUUID().toString(),
            TEST_LOCATION,
            1,
            System.currentTimeMillis(),
            LAST_ASSIGNED_COLUMN_ID,
            TEST_SCHEMA.schemaId(),
            ImmutableList.of(TEST_SCHEMA),
            SPEC_5.specId(),
            ImmutableList.of(SPEC_5),
            SPEC_5.lastAssignedFieldId(),
            SORT_ORDER_3.orderId(),
            ImmutableList.of(SORT_ORDER_3),
            ImmutableMap.of(),
            snapshotId2,
            null, // No snapshots provided directly
            () -> ImmutableList.of(snapshot1, snapshot2), // Lazy supplier
            ImmutableList.of(),
            ImmutableList.of(),
            ImmutableMap.of(
                SnapshotRef.MAIN_BRANCH, SnapshotRef.branchBuilder(snapshotId2).build()),
            ImmutableList.of(),
            ImmutableList.of(),
            0L,
            ImmutableList.of(),
            ImmutableList.of());

    // Create projection
    TableMetadata projected =
        TableMetadataProjection.create(
            base,
            snapshot -> {
              if (snapshot instanceof BaseSnapshot) {
                BaseSnapshot tSnapshot = (BaseSnapshot) snapshot;
                return new BaseSnapshot(
                    tSnapshot.sequenceNumber(),
                    tSnapshot.snapshotId(),
                    tSnapshot.parentId(),
                    tSnapshot.timestampMillis(),
                    tSnapshot.operation(),
                    ImmutableMap.of(), // Clear summary
                    tSnapshot.schemaId(),
                    tSnapshot.manifestListLocation(),
                    tSnapshot.firstRowId(),
                    tSnapshot.addedRows(),
                    tSnapshot.keyId());
              }
              return snapshot;
            });

    // IMPORTANT: Verify that currentSnapshotId() works WITHOUT triggering lazy loading
    assertThat(projected.currentSnapshotId()).isEqualTo(snapshotId2);

    // Verify that currentSnapshot() triggers loading and returns transformed snapshot
    Snapshot current = projected.currentSnapshot();
    assertThat(current).isNotNull();
    assertThat(current.snapshotId()).isEqualTo(snapshotId2);
    assertThat(current.summary()).isEmpty(); // Should be transformed (cleared)

    // Now verify all snapshots are accessible and transformed
    assertThat(projected.snapshots()).hasSize(2);
    assertThat(projected.snapshots().get(0).summary()).isEmpty();
    assertThat(projected.snapshots().get(1).summary()).isEmpty();
    assertThat(projected.snapshots().get(0).snapshotId()).isEqualTo(snapshotId1);
    assertThat(projected.snapshots().get(1).snapshotId()).isEqualTo(snapshotId2);
    assertThat(projected.snapshots().get(0).operation()).isEqualTo("append");
    assertThat(projected.snapshots().get(1).operation()).isEqualTo("overwrite");
  }

  @Test
  public void testNonSnapshotMethodsAreInherited() {
    long snapshotId = System.currentTimeMillis();
    Snapshot snapshot =
        new BaseSnapshot(
            0,
            snapshotId,
            null,
            snapshotId,
            "append",
            ImmutableMap.of(),
            TEST_SCHEMA.schemaId(),
            "file:/tmp/manifest.avro",
            null,
            null,
            null);

    Map<String, String> properties = ImmutableMap.of("key1", "value1", "key2", "value2");

    TableMetadata base =
        new TableMetadata(
            null,
            2,
            UUID.randomUUID().toString(),
            TEST_LOCATION,
            1,
            System.currentTimeMillis(),
            LAST_ASSIGNED_COLUMN_ID,
            TEST_SCHEMA.schemaId(),
            ImmutableList.of(TEST_SCHEMA),
            SPEC_5.specId(),
            ImmutableList.of(SPEC_5),
            SPEC_5.lastAssignedFieldId(),
            SORT_ORDER_3.orderId(),
            ImmutableList.of(SORT_ORDER_3),
            properties,
            snapshotId,
            ImmutableList.of(snapshot),
            null,
            ImmutableList.of(),
            ImmutableList.of(),
            ImmutableMap.of(SnapshotRef.MAIN_BRANCH, SnapshotRef.branchBuilder(snapshotId).build()),
            ImmutableList.of(),
            ImmutableList.of(),
            0L,
            ImmutableList.of(),
            ImmutableList.of());

    TableMetadata projected =
        TableMetadataProjection.create(base, s -> s); // Identity transformation

    // Verify all non-snapshot methods are properly inherited
    assertThat(projected.formatVersion()).isEqualTo(base.formatVersion());
    assertThat(projected.uuid()).isEqualTo(base.uuid());
    assertThat(projected.location()).isEqualTo(base.location());
    assertThat(projected.lastSequenceNumber()).isEqualTo(base.lastSequenceNumber());
    assertThat(projected.lastUpdatedMillis()).isEqualTo(base.lastUpdatedMillis());
    assertThat(projected.lastColumnId()).isEqualTo(base.lastColumnId());

    // Schema methods
    assertThat(projected.schema()).isEqualTo(base.schema());
    assertThat(projected.schemas()).isEqualTo(base.schemas());
    assertThat(projected.currentSchemaId()).isEqualTo(base.currentSchemaId());

    // Spec methods
    assertThat(projected.spec()).isEqualTo(base.spec());
    assertThat(projected.specs()).isEqualTo(base.specs());
    assertThat(projected.defaultSpecId()).isEqualTo(base.defaultSpecId());

    // Sort order methods
    assertThat(projected.sortOrder()).isEqualTo(base.sortOrder());
    assertThat(projected.sortOrders()).isEqualTo(base.sortOrders());
    assertThat(projected.defaultSortOrderId()).isEqualTo(base.defaultSortOrderId());

    // Properties
    assertThat(projected.properties()).isEqualTo(base.properties());
    assertThat(projected.property("key1", "default")).isEqualTo("value1");

    // Other metadata
    assertThat(projected.snapshotLog()).isEqualTo(base.snapshotLog());
    assertThat(projected.previousFiles()).isEqualTo(base.previousFiles());
    assertThat(projected.statisticsFiles()).isEqualTo(base.statisticsFiles());
    assertThat(projected.partitionStatisticsFiles()).isEqualTo(base.partitionStatisticsFiles());
  }

  @Test
  public void testSerializationAndDeserialization() {
    long snapshotId = System.currentTimeMillis();
    Map<String, String> summary = ImmutableMap.of("operation", "append", "records", "100");

    Snapshot snapshot =
        new BaseSnapshot(
            0,
            snapshotId,
            null,
            snapshotId,
            "append",
            summary,
            TEST_SCHEMA.schemaId(),
            "file:/tmp/manifest.avro",
            null,
            null,
            null);

    TableMetadata base =
        new TableMetadata(
            null,
            2,
            UUID.randomUUID().toString(),
            TEST_LOCATION,
            1,
            System.currentTimeMillis(),
            LAST_ASSIGNED_COLUMN_ID,
            TEST_SCHEMA.schemaId(),
            ImmutableList.of(TEST_SCHEMA),
            SPEC_5.specId(),
            ImmutableList.of(SPEC_5),
            SPEC_5.lastAssignedFieldId(),
            SORT_ORDER_3.orderId(),
            ImmutableList.of(SORT_ORDER_3),
            ImmutableMap.of(),
            snapshotId,
            ImmutableList.of(snapshot),
            null,
            ImmutableList.of(),
            ImmutableList.of(),
            ImmutableMap.of(SnapshotRef.MAIN_BRANCH, SnapshotRef.branchBuilder(snapshotId).build()),
            ImmutableList.of(),
            ImmutableList.of(),
            0L,
            ImmutableList.of(),
            ImmutableList.of());

    // Create projection that modifies snapshot
    TableMetadata projected =
        TableMetadataProjection.create(
            base,
            s -> {
              if (s instanceof BaseSnapshot) {
                BaseSnapshot bs = (BaseSnapshot) s;
                return new BaseSnapshot(
                    bs.sequenceNumber(),
                    bs.snapshotId(),
                    bs.parentId(),
                    bs.timestampMillis(),
                    bs.operation(),
                    ImmutableMap.of("filtered", "true"), // Modified summary
                    bs.schemaId(),
                    bs.manifestListLocation(),
                    bs.firstRowId(),
                    bs.addedRows(),
                    bs.keyId());
              }
              return s;
            });

    // Serialize the PROJECTED metadata (with transformed snapshots)
    String projectedJson = TableMetadataParser.toJson(projected);

    // Deserialize back
    TableMetadata deserialized = TableMetadataParser.fromJson(projectedJson);

    // The deserialized version should have the TRANSFORMED snapshot data
    // (not the original), because we serialized the projection
    assertThat(deserialized.snapshots()).hasSize(1);
    assertThat(deserialized.snapshots().get(0).snapshotId()).isEqualTo(snapshotId);
    assertThat(deserialized.snapshots().get(0).summary())
        .containsEntry("filtered", "true")
        .doesNotContainKey("records"); // Original key should be gone

    // But the base metadata should still have original data
    assertThat(base.snapshots().get(0).summary())
        .containsEntry("records", "100")
        .doesNotContainKey("filtered");

    // Verify other metadata is preserved correctly
    assertThat(deserialized.uuid()).isEqualTo(base.uuid());
    assertThat(deserialized.location()).isEqualTo(base.location());
    assertThat(deserialized.schema().asStruct()).isEqualTo(base.schema().asStruct());
  }

  @Test
  public void testProjectionIsTableMetadata() {
    long snapshotId = System.currentTimeMillis();
    Snapshot snapshot =
        new BaseSnapshot(
            0,
            snapshotId,
            null,
            snapshotId,
            "append",
            ImmutableMap.of(),
            TEST_SCHEMA.schemaId(),
            "file:/tmp/manifest.avro",
            null,
            null,
            null);

    TableMetadata base =
        new TableMetadata(
            null,
            2,
            UUID.randomUUID().toString(),
            TEST_LOCATION,
            1,
            System.currentTimeMillis(),
            LAST_ASSIGNED_COLUMN_ID,
            TEST_SCHEMA.schemaId(),
            ImmutableList.of(TEST_SCHEMA),
            SPEC_5.specId(),
            ImmutableList.of(SPEC_5),
            SPEC_5.lastAssignedFieldId(),
            SORT_ORDER_3.orderId(),
            ImmutableList.of(SORT_ORDER_3),
            ImmutableMap.of(),
            snapshotId,
            ImmutableList.of(snapshot),
            null,
            ImmutableList.of(),
            ImmutableList.of(),
            ImmutableMap.of(SnapshotRef.MAIN_BRANCH, SnapshotRef.branchBuilder(snapshotId).build()),
            ImmutableList.of(),
            ImmutableList.of(),
            0L,
            ImmutableList.of(),
            ImmutableList.of());

    TableMetadata projected = TableMetadataProjection.create(base, s -> s);

    // Verify projection is-a TableMetadata
    assertThat(projected).isInstanceOf(TableMetadata.class);

    // Can be used anywhere TableMetadata is expected
    TableMetadata metadata = projected;
    assertThat(metadata.snapshots()).isNotNull();
    assertThat(metadata.currentSnapshot()).isNotNull();
  }
}
