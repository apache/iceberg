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
import java.util.function.Consumer;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.TestBase;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class TestExpireSnapshotsActionV4 extends TestBase {
  private static final HadoopTables TABLES = new HadoopTables(new Configuration());
  private static final Schema SCHEMA =
      new Schema(
          optional(1, "c1", Types.IntegerType.get()), optional(2, "c2", Types.StringType.get()));
  private static final PartitionSpec SPEC = PartitionSpec.builderFor(SCHEMA).identity("c1").build();
  private static final DataFile FILE_A =
      DataFiles.builder(SPEC)
          .withPath("/path/to/data-a.parquet")
          .withFileSizeInBytes(10)
          .withPartitionPath("c1=0")
          .withRecordCount(1)
          .build();
  private static final DataFile FILE_B =
      DataFiles.builder(SPEC)
          .withPath("/path/to/data-b.parquet")
          .withFileSizeInBytes(10)
          .withPartitionPath("c1=1")
          .withRecordCount(1)
          .build();

  @TempDir private File tableDir;

  private enum EntryPoint {
    EXECUTE(ExpireSnapshotsSparkAction::execute),
    EXPIRE_FILES(ExpireSnapshotsSparkAction::expireFiles);

    private final Consumer<ExpireSnapshotsSparkAction> invocation;

    EntryPoint(Consumer<ExpireSnapshotsSparkAction> invocation) {
      this.invocation = invocation;
    }

    void invokeOn(ExpireSnapshotsSparkAction action) {
      invocation.accept(action);
    }
  }

  @ParameterizedTest
  @EnumSource(EntryPoint.class)
  void rejectsExpirationOnV4(EntryPoint entryPoint) {
    Table table = createTableWithTwoSnapshots(4);
    TableOperations ops = ((HasTableOperations) table).operations();
    long expirableSnapshotId = table.currentSnapshot().parentId();
    long currentSnapshotId = table.currentSnapshot().snapshotId();
    String metadataBefore = ops.current().metadataFileLocation();

    ExpireSnapshotsSparkAction action =
        SparkActions.get().expireSnapshots(table).expireSnapshotId(expirableSnapshotId);
    assertThatThrownBy(() -> entryPoint.invokeOn(action))
        .isInstanceOf(ValidationException.class)
        .hasMessage("Cannot expire snapshots: this action does not support v4 tables");

    table.refresh();
    assertThat(table.snapshot(expirableSnapshotId))
        .as("the expirable snapshot must be retained")
        .isNotNull();
    assertThat(table.currentSnapshot().snapshotId())
        .as("the current snapshot must be unchanged")
        .isEqualTo(currentSnapshotId);
    assertThat(ops.current().metadataFileLocation())
        .as("no new metadata file should be committed")
        .isEqualTo(metadataBefore);
  }

  @Test
  void rejectsOnStaleV3HandleAfterUpgradeToV4() {
    Table staleHandle = createTableWithTwoSnapshots(3);
    TableOperations staleOps = ((HasTableOperations) staleHandle).operations();
    long expirableSnapshotId = staleHandle.currentSnapshot().parentId();
    assertThat(staleOps.current().formatVersion()).isEqualTo(3);

    // upgrade to v4 through a second handle; the first handle keeps its cached v3 metadata
    Table upgradeHandle = TABLES.load(tableLocation());
    upgradeHandle.updateProperties().set(TableProperties.FORMAT_VERSION, "4").commit();

    assertThat(staleOps.current().formatVersion())
        .as("first handle should still see cached v3 metadata")
        .isEqualTo(3);

    assertThatThrownBy(
            () ->
                SparkActions.get()
                    .expireSnapshots(staleHandle)
                    .expireSnapshotId(expirableSnapshotId)
                    .execute())
        .isInstanceOf(ValidationException.class)
        .hasMessage("Cannot expire snapshots: this action does not support v4 tables");

    assertThat(TABLES.load(tableLocation()).snapshots())
        .as("no snapshot should be expired")
        .hasSize(2);
  }

  private Table createTableWithTwoSnapshots(int formatVersion) {
    Table table =
        TABLES.create(
            SCHEMA,
            SPEC,
            ImmutableMap.of(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion)),
            tableLocation());
    table.newAppend().appendFile(FILE_A).commit();
    table.newAppend().appendFile(FILE_B).commit();
    return table;
  }

  private String tableLocation() {
    return tableDir.toURI().toString();
  }
}
