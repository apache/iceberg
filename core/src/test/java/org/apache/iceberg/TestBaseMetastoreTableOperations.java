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

import org.apache.iceberg.BaseMetastoreOperations.CommitStatus;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

class TestBaseMetastoreTableOperations {

  private static final String TABLE_LOCATION = "file:/tmp/db/never_persisted";
  private static final String METADATA_LOCATION =
      TABLE_LOCATION + "/metadata/00000-uuid.metadata.json";

  private static final TableMetadata METADATA =
      TableMetadata.newTableMetadata(
          new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get())),
          PartitionSpec.unpartitioned(),
          TABLE_LOCATION,
          ImmutableMap.of(
              TableProperties.COMMIT_NUM_STATUS_CHECKS, "1",
              TableProperties.COMMIT_STATUS_CHECKS_MIN_WAIT_MS, "1",
              TableProperties.COMMIT_STATUS_CHECKS_MAX_WAIT_MS, "10",
              TableProperties.COMMIT_STATUS_CHECKS_TOTAL_WAIT_MS, "100"));

  /** Table operations for a table that was never persisted, so refresh yields null metadata. */
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
  }

  @Test
  void strictStatusCheckIsFailureWhenTableWasNeverPersisted() {
    NeverPersistedTableOperations ops = new NeverPersistedTableOperations();

    assertThat(ops.checkCommitStatusStrict(METADATA_LOCATION, METADATA))
        .isEqualTo(CommitStatus.FAILURE);
  }

  @Test
  void statusCheckIsUnknownWhenTableWasNeverPersisted() {
    NeverPersistedTableOperations ops = new NeverPersistedTableOperations();

    assertThat(ops.checkCommitStatus(METADATA_LOCATION, METADATA)).isEqualTo(CommitStatus.UNKNOWN);
  }
}
