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

import static org.apache.iceberg.TableProperties.AVRO_COMPRESSION;
import static org.apache.iceberg.TableProperties.AVRO_COMPRESSION_LEVEL;
import static org.apache.iceberg.TableProperties.ORC_COMPRESSION;
import static org.apache.iceberg.TableProperties.ORC_COMPRESSION_STRATEGY;
import static org.apache.iceberg.TableProperties.PARQUET_COMPRESSION;
import static org.apache.iceberg.TableProperties.PARQUET_COMPRESSION_LEVEL;
import static org.apache.iceberg.TableProperties.PARQUET_SHRED_VARIANTS;
import static org.apache.iceberg.TableProperties.PARQUET_VARIANT_BUFFER_SIZE;

import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.flink.annotation.Internal;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.FlinkWriteConf;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.SnapshotUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Internal
public class SinkUtil {

  static final long INITIAL_CHECKPOINT_ID = -1L;

  public static final String FLINK_JOB_ID = "flink.job-id";

  public static final String OPERATOR_ID = "flink.operator-id";
  public static final String MAX_COMMITTED_CHECKPOINT_ID = "flink.max-committed-checkpoint-id";

  private SinkUtil() {}

  private static final Logger LOG = LoggerFactory.getLogger(SinkUtil.class);

  static Set<Integer> checkAndGetEqualityFieldIds(Table table, List<String> equalityFieldColumns) {
    Set<Integer> equalityFieldIds = Sets.newHashSet(table.schema().identifierFieldIds());
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
      equalityFieldIds = Sets.newHashSet(equalityFieldSet);
    }
    return equalityFieldIds;
  }

  static long getMaxCommittedCheckpointId(
      Table table, String flinkJobId, String operatorId, String branch) {
    return maxCommittedCheckpointId(table, table.snapshot(branch), flinkJobId, operatorId);
  }

  /**
   * Returns the last checkpoint committed by the given job in the history of {@code head}, or -1
   * when there is none.
   *
   * @param operatorId the committing operator, or null to accept any operator of the job
   */
  static long maxCommittedCheckpointId(
      Table table, Snapshot head, String flinkJobId, @Nullable String operatorId) {
    Snapshot snapshot = head;
    while (snapshot != null) {
      Long checkpointId = committedCheckpointId(snapshot, flinkJobId, operatorId);
      if (checkpointId != null) {
        return checkpointId;
      }

      Long parentSnapshotId = snapshot.parentId();
      snapshot = parentSnapshotId != null ? table.snapshot(parentSnapshotId) : null;
    }

    return INITIAL_CHECKPOINT_ID;
  }

  /**
   * Returns the last checkpoint a snapshot committed, or null when the given job did not commit it.
   *
   * @param operatorId the committing operator, or null to accept any operator of the job
   */
  static Long committedCheckpointId(
      Snapshot snapshot, String flinkJobId, @Nullable String operatorId) {
    Map<String, String> summary = snapshot.summary();
    if (!flinkJobId.equals(summary.get(FLINK_JOB_ID))) {
      return null;
    }

    String snapshotOperatorId = summary.get(OPERATOR_ID);
    if (operatorId != null
        && snapshotOperatorId != null
        && !snapshotOperatorId.equals(operatorId)) {
      return null;
    }

    String value = summary.get(MAX_COMMITTED_CHECKPOINT_ID);
    return value != null ? Long.parseLong(value) : null;
  }

  /**
   * Whether the snapshots committed after {@code fromSnapshotId} up to {@code head} can still be
   * enumerated. They cannot once {@code fromSnapshotId} stopped being an ancestor of the branch,
   * typically because it expired.
   *
   * @param head the branch head, or null when the branch has no snapshot
   * @param fromSnapshotId the snapshot to start after, or null for the start of the history
   */
  static boolean isTraceable(Table table, Snapshot head, Long fromSnapshotId) {
    return fromSnapshotId == null
        || head == null
        || SnapshotUtil.isAncestorOf(head.snapshotId(), fromSnapshotId, table::snapshot);
  }

  /**
   * Based on the {@link FileFormat} overwrites the table level compression properties for the table
   * write.
   *
   * @param format The FileFormat to use
   * @param conf The write configuration
   * @param table The table to get the table level settings
   * @return The properties to use for writing
   */
  public static Map<String, String> writeProperties(
      FileFormat format, FlinkWriteConf conf, @Nullable Table table) {
    Map<String, String> writeProperties = Maps.newHashMap();
    if (table != null) {
      writeProperties.putAll(table.properties());
    }

    switch (format) {
      case PARQUET:
        writeProperties.put(PARQUET_COMPRESSION, conf.parquetCompressionCodec());
        String parquetCompressionLevel = conf.parquetCompressionLevel();
        if (parquetCompressionLevel != null) {
          writeProperties.put(PARQUET_COMPRESSION_LEVEL, parquetCompressionLevel);
        }

        writeProperties.put(PARQUET_SHRED_VARIANTS, String.valueOf(conf.parquetShredVariants()));
        writeProperties.put(
            PARQUET_VARIANT_BUFFER_SIZE, String.valueOf(conf.parquetVariantInferenceBufferSize()));

        break;
      case AVRO:
        writeProperties.put(AVRO_COMPRESSION, conf.avroCompressionCodec());
        String avroCompressionLevel = conf.avroCompressionLevel();
        if (avroCompressionLevel != null) {
          writeProperties.put(AVRO_COMPRESSION_LEVEL, conf.avroCompressionLevel());
        }

        break;
      case ORC:
        writeProperties.put(ORC_COMPRESSION, conf.orcCompressionCodec());
        writeProperties.put(ORC_COMPRESSION_STRATEGY, conf.orcCompressionStrategy());
        break;
      default:
        throw new IllegalArgumentException(String.format("Unknown file format %s", format));
    }

    return writeProperties;
  }
}
