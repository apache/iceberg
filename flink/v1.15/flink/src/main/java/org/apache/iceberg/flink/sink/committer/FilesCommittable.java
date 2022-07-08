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
package org.apache.iceberg.flink.sink.committer;

import java.io.Serializable;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;

public class FilesCommittable implements Serializable {
  private final WriteResult committable;
  private String jobId = "";
  private long checkpointId;
  private int subtaskId;

  public FilesCommittable(WriteResult committable) {
    this.committable = committable;
  }

  public FilesCommittable(WriteResult committable, String jobId, long checkpointId, int subtaskId) {
    this.committable = committable;
    this.jobId = jobId;
    this.checkpointId = checkpointId;
    this.subtaskId = subtaskId;
  }

  public WriteResult committable() {
    return committable;
  }

  public Long checkpointId() {
    return checkpointId;
  }

  public String jobID() {
    return jobId;
  }

  public void jobID(String newJobId) {
    this.jobId = newJobId;
  }

  public void checkpointId(long newCheckpointId) {
    this.checkpointId = newCheckpointId;
  }

  public void subtaskId(int newSubtaskId) {
    this.subtaskId = newSubtaskId;
  }

  public int subtaskId() {
    return subtaskId;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("committable", committable)
        .add("jobID", jobId)
        .add("checkpointId", checkpointId)
        .add("subtaskId", subtaskId)
        .toString();
  }
}
