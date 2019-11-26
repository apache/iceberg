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

package org.apache.iceberg.spark.source;

import java.util.Map;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotUpdate;
import org.apache.iceberg.Table;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.iceberg.TableProperties.WRITE_TARGET_FILE_SIZE_BYTES;
import static org.apache.iceberg.TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT;

public class IcebergStreamingWriter extends IcebergBatchWriter implements StreamingWrite {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergStreamingWriter.class);
  private static final String QUERY_ID_PROPERTY = "spark.sql.streaming.queryId";
  private static final String EPOCH_ID_PROPERTY = "spark.sql.streaming.epochId";

  private final String queryId;
  private final TableCapability writeBehavior;
  private final Table table;
  private final long targetFileSize;
  private final FileFormat format;
  private final Schema dsSchema;

  IcebergStreamingWriter(Table table, CaseInsensitiveStringMap options, String queryId, TableCapability writeBehavior,
      String applicationId, String wapId, Schema dsSchema) {
    super(table, options, writeBehavior, applicationId, wapId, dsSchema);
    this.queryId = queryId;
    this.writeBehavior = writeBehavior;
    this.table = table;
    this.format = getFileFormat(table.properties(), options);
    long tableTargetFileSize = PropertyUtil.propertyAsLong(
            table.properties(), WRITE_TARGET_FILE_SIZE_BYTES, WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);
    this.targetFileSize = options.getLong("target-file-size-bytes", tableTargetFileSize);
    this.dsSchema = dsSchema;
  }

  @Override
  public StreamingDataWriterFactory createStreamingWriterFactory() {
    return new StreamingWriterFactory(table.spec(), format, table.locationProvider(),
            table.properties(), table.io(), table.encryption(), targetFileSize, dsSchema);
  }

  private static class StreamingWriterFactory extends WriterFactory implements StreamingDataWriterFactory {

    StreamingWriterFactory(PartitionSpec spec, FileFormat format, LocationProvider locations,
                  Map<String, String> properties, FileIO fileIo, EncryptionManager encryptionManager,
                  long targetFileSize, Schema dsSchema) {
      super(spec, format, locations, properties, fileIo, encryptionManager, targetFileSize, dsSchema);
    }

    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskId, long epochId) {
      return  super.createWriter(partitionId, taskId, epochId);
    }
  }

  @Override
  public void commit(long epochId, WriterCommitMessage[] messages) {
    LOG.info("Committing epoch {} for query {} in {} mode", epochId, queryId, writeBehavior);

    table().refresh();
    Long lastCommittedEpochId = getLastCommittedEpochId();
    if (lastCommittedEpochId != null && epochId <= lastCommittedEpochId) {
      LOG.info("Skipping epoch {} for query {} as it was already committed", epochId, queryId);
      return;
    }

    if (writeBehavior.equals(TableCapability.TRUNCATE)) {
      OverwriteFiles overwriteFiles = table().newOverwrite();
      overwriteFiles.overwriteByRowFilter(Expressions.alwaysTrue());
      int numFiles = 0;
      for (DataFile file : files(messages)) {
        overwriteFiles.addFile(file);
        numFiles++;
      }
      commit(overwriteFiles, epochId, numFiles, "streaming complete overwrite");
    } else if (writeBehavior.equals(TableCapability.STREAMING_WRITE)) {
      AppendFiles append = table().newFastAppend();
      int numFiles = 0;
      for (DataFile file : files(messages)) {
        append.appendFile(file);
        numFiles++;
      }
      commit(append, epochId, numFiles, "streaming append");
    } else {
      throw new IllegalArgumentException("Iceberg doen't support write behavior " + writeBehavior + " for now");
    }
  }

  private <T> void commit(SnapshotUpdate<T> snapshotUpdate, long epochId, int numFiles, String description) {
    snapshotUpdate.set(QUERY_ID_PROPERTY, queryId);
    snapshotUpdate.set(EPOCH_ID_PROPERTY, Long.toString(epochId));
    commitOperation(snapshotUpdate, numFiles, description);
  }

  @Override
  public void abort(long epochId, WriterCommitMessage[] messages) {
    abort(messages);
  }

  private Long getLastCommittedEpochId() {
    Snapshot snapshot = table().currentSnapshot();
    Long lastCommittedEpochId = null;
    while (snapshot != null) {
      Map<String, String> summary = snapshot.summary();
      String snapshotQueryId = summary.get(QUERY_ID_PROPERTY);
      if (queryId.equals(snapshotQueryId)) {
        lastCommittedEpochId = Long.valueOf(summary.get(EPOCH_ID_PROPERTY));
        break;
      }
      Long parentSnapshotId = snapshot.parentId();
      snapshot = parentSnapshotId != null ? table().snapshot(parentSnapshotId) : null;
    }
    return lastCommittedEpochId;
  }
}
