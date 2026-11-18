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
package org.apache.iceberg.spark.actions;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotParser;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.TestBase;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestExpireSnapshotsActionV4 extends TestBase {
  private static final HadoopTables TABLES = new HadoopTables(new Configuration());
  private static final Schema SCHEMA = new Schema(optional(1, "id", Types.IntegerType.get()));
  private static final DataFile FILE_A =
      DataFiles.builder(PartitionSpec.unpartitioned())
          .withPath("/path/to/data-a.parquet")
          .withFileSizeInBytes(10)
          .withRecordCount(1)
          .build();
  private static final DataFile FILE_B =
      DataFiles.builder(PartitionSpec.unpartitioned())
          .withPath("/path/to/data-b.parquet")
          .withFileSizeInBytes(10)
          .withRecordCount(1)
          .build();
  private static final String UNSUPPORTED_MESSAGE =
      "Cannot expire snapshots: this action does not support tables with root manifests";

  @TempDir private File tableDir;

  @Test
  void rejectsExpirationWithRootManifestSnapshot() {
    Table table = createTableWithTwoSnapshots();
    long expirableSnapshotId = table.currentSnapshot().parentId();
    commitRootManifestSnapshot(table);
    TableOperations ops = ((HasTableOperations) table).operations();
    String metadataBefore = ops.current().metadataFileLocation();

    assertThatThrownBy(
            () ->
                SparkActions.get()
                    .expireSnapshots(table)
                    .expireSnapshotId(expirableSnapshotId)
                    .execute())
        .isInstanceOf(ValidationException.class)
        .hasMessage(UNSUPPORTED_MESSAGE);

    assertThat(ops.refresh().metadataFileLocation())
        .as("no new metadata file should be committed")
        .isEqualTo(metadataBefore);
  }

  @Test
  void rejectsOnStaleHandleAfterRootManifestSnapshot() {
    Table staleHandle = createTableWithTwoSnapshots();
    long expirableSnapshotId = staleHandle.currentSnapshot().parentId();

    // commit through a second handle; the first handle keeps its cached metadata
    commitRootManifestSnapshot(TABLES.load(tableLocation()));
    assertThat(staleHandle.snapshots()).as("stale handle should not see the commit").hasSize(2);

    assertThatThrownBy(
            () ->
                SparkActions.get()
                    .expireSnapshots(staleHandle)
                    .expireSnapshotId(expirableSnapshotId)
                    .execute())
        .isInstanceOf(ValidationException.class)
        .hasMessage(UNSUPPORTED_MESSAGE);

    assertThat(TABLES.load(tableLocation()).snapshots())
        .as("no snapshot should be expired")
        .hasSize(3);
  }

  private Table createTableWithTwoSnapshots() {
    Table table =
        TABLES.create(
            SCHEMA,
            PartitionSpec.unpartitioned(),
            ImmutableMap.of(TableProperties.FORMAT_VERSION, "4"),
            tableLocation());
    table.newAppend().appendFile(FILE_A).commit();
    table.newAppend().appendFile(FILE_B).commit();
    return table;
  }

  // there is no writer that produces root manifests yet, so commit a hand-built snapshot
  private static void commitRootManifestSnapshot(Table table) {
    TableOperations ops = ((HasTableOperations) table).operations();
    TableMetadata base = ops.current();
    Snapshot parent = base.currentSnapshot();
    Snapshot rootManifestSnapshot =
        SnapshotParser.fromJson(
            String.format(
                "{\"sequence-number\": %d, \"snapshot-id\": %d, \"parent-snapshot-id\": %d, "
                    + "\"timestamp-ms\": %d, \"summary\": {\"operation\": \"append\"}, "
                    + "\"schema-id\": %d, \"first-row-id\": %d, \"added-rows\": 0, "
                    + "\"root-manifest\": \"file:/nonexistent/root-manifest.parquet\"}",
                base.lastSequenceNumber() + 1,
                parent.snapshotId() + 1,
                parent.snapshotId(),
                parent.timestampMillis() + 1,
                base.currentSchemaId(),
                base.nextRowId()));
    ops.commit(
        base,
        TableMetadata.buildFrom(base)
            .setBranchSnapshot(rootManifestSnapshot, SnapshotRef.MAIN_BRANCH)
            .build());
  }

  private String tableLocation() {
    return tableDir.toURI().toString();
  }
}
