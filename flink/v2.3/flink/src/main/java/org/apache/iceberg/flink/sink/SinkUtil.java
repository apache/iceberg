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
import java.util.function.Predicate;
import javax.annotation.Nullable;
import org.apache.flink.annotation.Internal;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotAncestryValidator;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.flink.FlinkWriteConf;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Internal
public class SinkUtil {

  static final long INITIAL_CHECKPOINT_ID = -1L;

  public static final String FLINK_JOB_ID = "flink.job-id";
  public static final String SINK_ID = "flink.sink-id";

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

  static long getMaxCommittedCheckpointIdBySink(
      Table table, String sinkId, String operatorId, String branch) {
    Snapshot snapshot = table.snapshot(branch);
    long lastCommittedCheckpointId = INITIAL_CHECKPOINT_ID;

    while (snapshot != null) {
      Map<String, String> summary = snapshot.summary();
      if (writtenBySameSink(summary, sinkId, operatorId)) {
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

  public static boolean writtenBySameSink(
      Map<String, String> summary, String sinkId, String operatorId) {
    String snapshotOperatorId = summary.get(OPERATOR_ID);
    if (snapshotOperatorId != null && !snapshotOperatorId.equals(operatorId)) {
      return false;
    }

    // Snapshots written before the sinkId was introduced are identified by the jobId
    String snapshotSinkId = summary.get(SINK_ID);
    return sinkId.equals(snapshotSinkId != null ? snapshotSinkId : summary.get(FLINK_JOB_ID));
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

  /**
   * Validates, inside the commit, that the target branch does not already contain a commit for the
   * staged checkpoint.
   *
   * <p>The committers read the max committed checkpoint ID before they commit. Another commit for
   * the same checkpoint can land after that read, for example when a slow commit succeeds after the
   * committer gave up and the request is redelivered on recovery, or when an old and a new job
   * commit the same restored checkpoint. The base ancestry is refreshed before every commit
   * attempt, so this validator sees such a commit and rejects the duplicate with a {@link
   * MaxCommittedCheckpointMismatchException}.
   *
   * <p>Shared by {@link IcebergCommitter} and {@code DynamicCommitter}, which identify their own
   * snapshots differently, so the caller supplies the {@code writtenBySameSink} predicate.
   */
  @Internal
  public static class MaxCommittedCheckpointIdValidator implements SnapshotAncestryValidator {
    private final long stagedCheckpointId;
    private final Predicate<Map<String, String>> writtenBySameSink;

    /**
     * @param stagedCheckpointId the checkpoint ID that is being committed
     * @param writtenBySameSink returns true for snapshot summaries that the committing sink wrote
     */
    public MaxCommittedCheckpointIdValidator(
        long stagedCheckpointId, Predicate<Map<String, String>> writtenBySameSink) {
      this.stagedCheckpointId = stagedCheckpointId;
      this.writtenBySameSink = writtenBySameSink;
    }

    @Override
    public boolean validate(Iterable<Snapshot> baseSnapshots) {
      long maxCommittedCheckpointId = INITIAL_CHECKPOINT_ID;
      for (Snapshot ancestor : baseSnapshots) {
        Map<String, String> summary = ancestor.summary();
        if (writtenBySameSink.test(summary)) {
          String value = summary.get(MAX_COMMITTED_CHECKPOINT_ID);
          if (value != null) {
            maxCommittedCheckpointId = Long.parseLong(value);
            break;
          }
        }
      }

      if (maxCommittedCheckpointId >= stagedCheckpointId) {
        throw new MaxCommittedCheckpointMismatchException();
      }

      return true;
    }
  }

  /**
   * Thrown by {@link MaxCommittedCheckpointIdValidator} when the branch already contains a commit
   * for the staged checkpoint. Committers catch it to skip the duplicate commit.
   */
  public static class MaxCommittedCheckpointMismatchException extends ValidationException {
    public MaxCommittedCheckpointMismatchException() {
      super("Table already contains staged changes.");
    }
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
