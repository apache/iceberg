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

  @TempDir private File tableDir;

  @Test
  void rejectsExpirationOnV4() {
    Table table = createTableWithTwoSnapshots(4);
    TableOperations ops = ((HasTableOperations) table).operations();
    long expirableSnapshotId = table.currentSnapshot().parentId();
    String metadataBefore = ops.current().metadataFileLocation();

    assertThatThrownBy(
            () ->
                SparkActions.get()
                    .expireSnapshots(table)
                    .expireSnapshotId(expirableSnapshotId)
                    .execute())
        .isInstanceOf(ValidationException.class)
        .hasMessage("Cannot expire snapshots: this action does not support v4 tables");

    assertThat(ops.refresh().metadataFileLocation())
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
    TABLES
        .load(tableLocation())
        .updateProperties()
        .set(TableProperties.FORMAT_VERSION, "4")
        .commit();

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
            PartitionSpec.unpartitioned(),
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
