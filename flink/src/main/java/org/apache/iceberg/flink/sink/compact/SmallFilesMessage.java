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

package org.apache.iceberg.flink.sink.compact;

import java.io.Serializable;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;

public class SmallFilesMessage {
  private SmallFilesMessage() {
  }

  public interface CommonControllerMessage extends Serializable {
  }

  //  checkpoint end message
  public static class EndCheckpoint implements CommonControllerMessage {
    private final long checkpointId;
    private final int taskId;
    private final int numberOfTasks;

    //  checkpoint end message
    public EndCheckpoint(long checkpointId, int taskId, int numberOfTasks) {
      this.checkpointId = checkpointId;
      this.taskId = taskId;
      this.numberOfTasks = numberOfTasks;
    }

    public long getCheckpointId() {
      return checkpointId;
    }

    public int getTaskId() {
      return taskId;
    }

    public int getNumberOfTasks() {
      return numberOfTasks;
    }
  }

  //  Compaction Unit information, contains a CombinedScanTask object to compact
  public static class CompactionUnit implements CommonControllerMessage {
    private int unitId;
    private CombinedScanTask combinedScanTask;

    public CompactionUnit(CombinedScanTask combinedScanTask, int taskId) {
      this.combinedScanTask = combinedScanTask;
      this.unitId = taskId;
    }

    public boolean isTaskMessage(int taskNumber, int taskId) {
      return unitId % taskNumber == taskId;
    }

    public int getUnitId() {
      return this.unitId;
    }

    public CombinedScanTask getCombinedScanTask() {
      return this.combinedScanTask;
    }
  }

  //  Compaction end message
  public static class EndCompaction implements CommonControllerMessage {
    private final long checkpointId;
    private final long startingSnapshotId;

    public EndCompaction(long checkpointId, long startingSnapshotId) {
      this.checkpointId = checkpointId;
      this.startingSnapshotId = startingSnapshotId;
    }

    public long getCheckpointId() {
      return checkpointId;
    }
    public long getStartingSnapshotId() {
      return startingSnapshotId;
    }
  }

  //  Compaction commit information, contains files to delete and add
  public static class CompactCommitInfo implements CommonControllerMessage {
    private long checkpointId;
    private int taskId;
    private int numberOfTasks;

    private long startingSnapshotId;
    private List<DataFile> dataFiles;
    private List<DataFile> currentDataFiles;

    public CompactCommitInfo() {
    }

    public CompactCommitInfo(
        long checkpointId,
        int taskId,
        int numberOfTasks,
        long startingSnapshotId,
        List<DataFile> dataFiles,
        List<DataFile> currentDataFiles) {
      this.checkpointId = checkpointId;
      this.taskId = taskId;
      this.numberOfTasks = numberOfTasks;
      this.startingSnapshotId = startingSnapshotId;
      this.dataFiles = dataFiles;
      this.currentDataFiles = currentDataFiles;
    }

    public long getCheckpointId() {
      return checkpointId;
    }

    public void setCheckpointId(long checkpointId) {
      this.checkpointId = checkpointId;
    }

    public int getTaskId() {
      return taskId;
    }

    public void setTaskId(int taskId) {
      this.taskId = taskId;
    }

    public int getNumberOfTasks() {
      return numberOfTasks;
    }

    public void setNumberOfTasks(int numberOfTasks) {
      this.numberOfTasks = numberOfTasks;
    }

    public long getStartingSnapshotId() {
      return startingSnapshotId;
    }

    public void setStartingSnapshotId(int startingSnapshotId) {
      this.startingSnapshotId = startingSnapshotId;
    }

    public List<DataFile> getDataFiles() {
      return dataFiles;
    }

    public void setDataFiles(List<DataFile> addedDataFiles) {
      this.dataFiles = addedDataFiles;
    }

    public List<DataFile> getCurrentDataFiles() {
      return currentDataFiles;
    }

    public void setCurrentDataFiles(List<DataFile> currentDataFiles) {
      this.currentDataFiles = currentDataFiles;
    }
  }

  //  Snapshot unit information, contains expire files to delete
  public static class SnapshotsUnit implements CommonControllerMessage {
    private final int unitId;
    private final Set<String> deletedFiles;

    public SnapshotsUnit(Set<String> files, int taskId) {
      this.deletedFiles = files;
      this.unitId = taskId;
    }

    public boolean isTaskMessage(int taskNumber, int taskId) {
      return unitId % taskNumber == taskId;
    }

    public int getUnitId() {
      return this.unitId;
    }

    public Set<String> getDeletedFiles() {
      return this.deletedFiles;
    }
  }

  //  expire snapshot end message
  public static class EndExpireSnapshot implements CommonControllerMessage {
    private final long checkpointId;

    public EndExpireSnapshot(long checkpointId) {
      this.checkpointId = checkpointId;
    }

    public long getCheckpointId() {
      return checkpointId;
    }
  }

}
