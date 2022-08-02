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
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotUpdate;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.spark.SparkWriteConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamingWriter extends Writer implements StreamWriter {

  private static final Logger LOG = LoggerFactory.getLogger(StreamingWriter.class);
  private static final String QUERY_ID_PROPERTY = "spark.sql.streaming.queryId";
  private static final String EPOCH_ID_PROPERTY = "spark.sql.streaming.epochId";

  private final String queryId;
  private final OutputMode mode;

  StreamingWriter(
      SparkSession spark,
      Table table,
      SparkWriteConf writeConf,
      String queryId,
      OutputMode mode,
      String applicationId,
      Schema writeSchema,
      StructType dsSchema) {
    super(spark, table, writeConf, false, applicationId, writeSchema, dsSchema);
    this.queryId = queryId;
    this.mode = mode;
  }

  @Override
  public void commit(long epochId, WriterCommitMessage[] messages) {
    LOG.info("Committing epoch {} for query {} in {} mode", epochId, queryId, mode);

    table().refresh();
    Long lastCommittedEpochId = getLastCommittedEpochId();
    if (lastCommittedEpochId != null && epochId <= lastCommittedEpochId) {
      LOG.info("Skipping epoch {} for query {} as it was already committed", epochId, queryId);
      return;
    }

    if (mode == OutputMode.Complete()) {
      OverwriteFiles overwriteFiles = table().newOverwrite();
      overwriteFiles.overwriteByRowFilter(Expressions.alwaysTrue());
      int numFiles = 0;
      for (DataFile file : files(messages)) {
        overwriteFiles.addFile(file);
        numFiles++;
      }
      commit(overwriteFiles, epochId, numFiles, "streaming complete overwrite");
    } else {
      AppendFiles append = table().newFastAppend();
      int numFiles = 0;
      for (DataFile file : files(messages)) {
        append.appendFile(file);
        numFiles++;
      }
      commit(append, epochId, numFiles, "streaming append");
    }
  }

  private <T> void commit(
      SnapshotUpdate<T> snapshotUpdate, long epochId, int numFiles, String description) {
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
