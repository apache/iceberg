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

package org.apache.iceberg.flink.sink.expire.snapshot;

import java.io.Serializable;
import java.util.Set;

public class ExpireSnapshotMessage {
  private ExpireSnapshotMessage() {
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
