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
package org.apache.iceberg.flink.sink;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.flink.util.Preconditions;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SinkUtil {

  private static final long INITIAL_CHECKPOINT_ID = -1L;

  public static final String FLINK_JOB_ID = "flink.job-id";

  public static final String OPERATOR_ID = "flink.operator-id";
  public static final String MAX_COMMITTED_CHECKPOINT_ID = "flink.max-committed-checkpoint-id";

  private SinkUtil() {}

  private static final Logger LOG = LoggerFactory.getLogger(SinkUtil.class);

  static List<Integer> checkAndGetEqualityFieldIds(Table table, List<String> equalityFieldColumns) {
    List<Integer> equalityFieldIds = Lists.newArrayList(table.schema().identifierFieldIds());
    if (equalityFieldColumns != null && !equalityFieldColumns.isEmpty()) {
      Set<Integer> equalityFieldSet = Sets.newHashSetWithExpectedSize(equalityFieldColumns.size());
      for (String column : equalityFieldColumns) {
        org.apache.iceberg.types.Types.NestedField field = table.schema().findField(column);
        Preconditions.checkNotNull(
            field,
            "Missing required equality field column '%s' in table schema %s",
            column,
            table.schema());
        equalityFieldSet.add(field.fieldId());
      }

      if (!equalityFieldSet.equals(table.schema().identifierFieldIds())) {
        LOG.warn(
            "The configured equality field column IDs {} are not matched with the schema identifier field IDs"
                + " {}, use job specified equality field columns as the equality fields by default.",
            equalityFieldSet,
            table.schema().identifierFieldIds());
      }
      equalityFieldIds = Lists.newArrayList(equalityFieldSet);
    }
    return equalityFieldIds;
  }

  static long getMaxCommittedCheckpointId(
      Table table, String flinkJobId, String operatorId, String branch) {
    Snapshot snapshot = table.snapshot(branch);
    long lastCommittedCheckpointId = INITIAL_CHECKPOINT_ID;

    while (snapshot != null) {
      Map<String, String> summary = snapshot.summary();
      String snapshotFlinkJobId = summary.get(FLINK_JOB_ID);
      String snapshotOperatorId = summary.get(OPERATOR_ID);
      if (flinkJobId.equals(snapshotFlinkJobId)
          && (snapshotOperatorId == null || snapshotOperatorId.equals(operatorId))) {
        String value = summary.get(MAX_COMMITTED_CHECKPOINT_ID);
        if (value != null) {
          lastCommittedCheckpointId = Long.parseLong(value);
          break;
        }
      }
      Long parentSnapshotId = snapshot.parentId();
      snapshot = parentSnapshotId != null ? table.snapshot(parentSnapshotId) : null;
    }

    return lastCommittedCheckpointId;
  }
}
