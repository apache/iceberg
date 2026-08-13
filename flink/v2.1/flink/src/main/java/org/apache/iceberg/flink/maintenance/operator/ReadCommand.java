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

/**
 * Envelope from the {@code EqualityConvertPlanner} to the {@code EqualityConvertReader}, wrapping
 * an Iceberg {@link ContentScanTask} plus the metadata the reader needs to process it.
 *
 * <p>The wrapped task is either:
 *
 * <ul>
 *   <li>A {@link FileScanTask} for data files: native tasks from {@code table.newScan()} for main
 *       reindex, or a {@link FlinkAddedRowsScanTask} wrapper for bare {@link
 *       org.apache.iceberg.DataFile}s from {@code SnapshotChanges#addedDataFiles}.
 *   <li>An {@link EqualityDeleteFileScanTask} for equality delete files.
 * </ul>
 *
 * <p>The equality field IDs are static for the lifetime of the job and carried on the reader
 * itself, not per-record.
 *
 * <p>{@code mainSnapshotId} is sent for diagnostic output.
 *
 * <p>{@code mainSequenceNumber} is used by the index to order eager evictions by.
 *
 * <p>{@code dataSequenceNumber} is the wrapped file's sequence number (data file or equality
 * delete), propagated to the worker so a delete only deletes rows older than itself.
 *
 * <p>{@code staging} is true for a staging snapshot's new data rows, which the index defers until
 * after this cycle's delete resolves; false for main reindex data and equality deletes.
 */
@Internal
public record ReadCommand(
    ContentScanTask<?> task,
    Long mainSnapshotId,
    Long mainSequenceNumber,
    long dataSequenceNumber,
    boolean staging)
    implements Serializable {

  public static ReadCommand dataFile(
      FileScanTask task, Long mainSnapshotId, Long mainSequenceNumber, long dataSequenceNumber) {
    return new ReadCommand(task, mainSnapshotId, mainSequenceNumber, dataSequenceNumber, false);
  }

  public static ReadCommand stagingDataFile(
      FileScanTask task, Long mainSnapshotId, Long mainSequenceNumber, long dataSequenceNumber) {
    return new ReadCommand(task, mainSnapshotId, mainSequenceNumber, dataSequenceNumber, true);
  }

  public static ReadCommand eqDeleteFile(
      DeleteFile file,
      PartitionSpec spec,
      Long mainSnapshotId,
      Long mainSequenceNumber,
      long dataSequenceNumber) {
    return new ReadCommand(
        new EqualityDeleteFileScanTask(file, spec),
        mainSnapshotId,
        mainSequenceNumber,
        dataSequenceNumber,
        false);
  }
}
