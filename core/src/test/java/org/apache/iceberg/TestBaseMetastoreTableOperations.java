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
import org.apache.iceberg.BaseMetastoreOperations.CommitStatus;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestBaseMetastoreTableOperations {

  private static final Schema SCHEMA =
      new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));

  private static final Map<String, String> FAST_STATUS_CHECKS =
      ImmutableMap.of(
          TableProperties.COMMIT_NUM_STATUS_CHECKS, "1",
          TableProperties.COMMIT_STATUS_CHECKS_MIN_WAIT_MS, "1",
          TableProperties.COMMIT_STATUS_CHECKS_MAX_WAIT_MS, "10",
          TableProperties.COMMIT_STATUS_CHECKS_TOTAL_WAIT_MS, "100");

  /**
   * Mimics metastore-backed table operations for a table that was never persisted to the catalog,
   * e.g. when a CREATE TABLE commit fails before the table is stored in the metastore. Like {@code
   * HiveTableOperations#doRefresh()}, a missing table is not an error when no metadata location is
   * known, and refreshing from a null metadata location leaves the current metadata null.
   */
  private static class NeverPersistedTableOperations extends BaseMetastoreTableOperations {

    @Override
    protected String tableName() {
      return "db.never_persisted";
    }

    @Override
    public FileIO io() {
      return null;
    }

    @Override
    protected void doRefresh() {
      refreshFromMetadataLocation(null, 1);
    }

    private CommitStatus strictStatus(String newMetadataLocation, TableMetadata config) {
      return checkCommitStatusStrict(newMetadataLocation, config);
    }

    private CommitStatus status(String newMetadataLocation, TableMetadata config) {
      return checkCommitStatus(newMetadataLocation, config);
    }
  }

  @Test
  public void strictStatusCheckIsFailureWhenTableWasNeverPersisted() {
    NeverPersistedTableOperations ops = new NeverPersistedTableOperations();
    TableMetadata metadata =
        TableMetadata.newTableMetadata(
            SCHEMA,
            PartitionSpec.unpartitioned(),
            "file:/tmp/db/never_persisted",
            FAST_STATUS_CHECKS);

    assertThat(
            ops.strictStatus(
                "file:/tmp/db/never_persisted/metadata/00000-uuid.metadata.json", metadata))
        .isEqualTo(CommitStatus.FAILURE);
  }

  @Test
  public void statusCheckIsUnknownWhenTableWasNeverPersisted() {
    NeverPersistedTableOperations ops = new NeverPersistedTableOperations();
    TableMetadata metadata =
        TableMetadata.newTableMetadata(
            SCHEMA,
            PartitionSpec.unpartitioned(),
            "file:/tmp/db/never_persisted",
            FAST_STATUS_CHECKS);

    assertThat(
            ops.status("file:/tmp/db/never_persisted/metadata/00000-uuid.metadata.json", metadata))
        .isEqualTo(CommitStatus.UNKNOWN);
  }
}
