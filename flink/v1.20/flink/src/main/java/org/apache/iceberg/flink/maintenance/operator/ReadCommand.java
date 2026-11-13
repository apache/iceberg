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
package org.apache.iceberg.flink.maintenance.operator;

import java.io.Serializable;
import org.apache.flink.annotation.Internal;
import org.apache.iceberg.ContentScanTask;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Envelope from the {@code EqualityConvertPlanner} to the {@code EqualityConvertReader}, wrapping
 * an Iceberg {@link ContentScanTask} plus the metadata the reader needs to process it.
 *
 * <p>The wrapped task is either:
 *
 * <ul>
 *   <li>A {@link FileScanTask} for data files: native tasks from {@code table.newScan()} for a full
 *       rebuild of the index, or a {@link FlinkAddedRowsScanTask} wrapper for bare {@link
 *       org.apache.iceberg.DataFile}s from {@code SnapshotChanges}.
 *   <li>An {@link EqualityDeleteFileScanTask} for equality delete files.
 * </ul>
 *
 * <p>The equality field IDs are static for the lifetime of the job and carried on the reader
 * itself, not per-record.
 *
 * <p>{@code mainSnapshotId} is sent for diagnostic output.
 *
 * <p>{@code indexGeneration} is used by the index to order eager evictions by.
 *
 * <p>{@code dataSequenceNumber} is the wrapped file's sequence number (data file or equality
 * delete), propagated to the worker so a delete only deletes rows older than itself.
 *
 * <p>{@code type} selects the {@link IndexCommand} the reader emits per row and pairs with the task
 * subtype, which the constructor enforces.
 */
@Internal
public record ReadCommand(
    ContentScanTask<?> task,
    Long mainSnapshotId,
    Long indexGeneration,
    long dataSequenceNumber,
    Type type)
    implements Serializable {

  /**
   * {@link #STAGING_DATA_FILE} rows join the index only after this cycle's delete resolves. The
   * rows of a {@link #REMOVED_DATA_FILE} leave it.
   */
  public enum Type {
    DATA_FILE,
    STAGING_DATA_FILE,
    REMOVED_DATA_FILE,
    EQ_DELETE_FILE
  }

  public ReadCommand {
    Class<?> required =
        type == Type.EQ_DELETE_FILE ? EqualityDeleteFileScanTask.class : FileScanTask.class;
    Preconditions.checkArgument(
        required.isInstance(task),
        "%s requires a %s, got %s",
        type,
        required.getSimpleName(),
        task.getClass().getName());
  }

  public static ReadCommand dataFile(
      FileScanTask task, Long mainSnapshotId, Long indexGeneration, long dataSequenceNumber) {
    return new ReadCommand(
        task, mainSnapshotId, indexGeneration, dataSequenceNumber, Type.DATA_FILE);
  }

  public static ReadCommand stagingDataFile(
      FileScanTask task, Long mainSnapshotId, Long indexGeneration, long dataSequenceNumber) {
    return new ReadCommand(
        task, mainSnapshotId, indexGeneration, dataSequenceNumber, Type.STAGING_DATA_FILE);
  }

  public static ReadCommand removedDataFile(
      FileScanTask task, Long mainSnapshotId, Long indexGeneration, long dataSequenceNumber) {
    return new ReadCommand(
        task, mainSnapshotId, indexGeneration, dataSequenceNumber, Type.REMOVED_DATA_FILE);
  }

  public static ReadCommand eqDeleteFile(
      DeleteFile file,
      PartitionSpec spec,
      Long mainSnapshotId,
      Long indexGeneration,
      long dataSequenceNumber) {
    return new ReadCommand(
        new EqualityDeleteFileScanTask(file, spec),
        mainSnapshotId,
        indexGeneration,
        dataSequenceNumber,
        Type.EQ_DELETE_FILE);
  }
}
