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
package org.apache.iceberg.mr.hive;

import java.util.Objects;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.JobContextImpl;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.hadoop.mapred.TaskAttemptContextImpl;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapreduce.JobID;

public class TezUtil {

  private static final String TASK_ATTEMPT_ID_KEY = "mapred.task.id";
  // TezProcessor (Hive) propagates the vertex id under this key - available during Task commit
  // phase
  private static final String TEZ_VERTEX_ID_HIVE = "hive.tez.vertex.index";
  // MROutputCommitter (Tez) propagates the vertex id under this key - available during DAG/Vertex
  // commit phase
  private static final String TEZ_VERTEX_ID_DAG = "mapreduce.task.vertex.id";

  /**
   * If the Tez vertex id is present in config, creates a new jobContext by appending the Tez vertex
   * id to the jobID. For the rationale behind this enrichment, please refer to point #1 in the docs
   * of {@link TaskAttemptWrapper}.
   *
   * @param jobContext original jobContext to be enriched
   * @return enriched jobContext
   */
  public static JobContext enrichContextWithVertexId(JobContext jobContext) {
    String vertexId = jobContext.getJobConf().get(TEZ_VERTEX_ID_DAG);
    if (vertexId != null) {
      JobID jobID = getJobIDWithVertexAppended(jobContext.getJobID(), vertexId);
      return new JobContextImpl(jobContext.getJobConf(), jobID, jobContext.getProgressible());
    } else {
      return jobContext;
    }
  }

  /**
   * Creates a new taskAttemptContext by replacing the taskAttemptID with a wrapped object. For the
   * rationale behind this enrichment, please refer to point #2 in the docs of {@link
   * TaskAttemptWrapper}.
   *
   * @param taskAttemptContext original taskAttemptContext to be enriched
   * @return enriched taskAttemptContext
   */
  public static TaskAttemptContext enrichContextWithAttemptWrapper(
      TaskAttemptContext taskAttemptContext) {
    TaskAttemptID wrapped = TezUtil.taskAttemptWrapper(taskAttemptContext.getTaskAttemptID());
    return new TaskAttemptContextImpl(taskAttemptContext.getJobConf(), wrapped);
  }

  public static TaskAttemptID taskAttemptWrapper(TaskAttemptID attemptID) {
    return new TaskAttemptWrapper(attemptID, "");
  }

  public static TaskAttemptID taskAttemptWrapper(JobConf jc) {
    return new TaskAttemptWrapper(
        TaskAttemptID.forName(jc.get(TASK_ATTEMPT_ID_KEY)), jc.get(TEZ_VERTEX_ID_HIVE));
  }

  private static JobID getJobIDWithVertexAppended(JobID jobID, String vertexId) {
    if (vertexId != null && !vertexId.isEmpty()) {
      return new JobID(jobID.getJtIdentifier() + vertexId, jobID.getId());
    } else {
      return jobID;
    }
  }

  private TezUtil() {}

  /**
   * Subclasses {@link org.apache.hadoop.mapred.TaskAttemptID}. It has two main purposes: 1. Provide
   * a way to append an optional vertex id to the Job ID. This is needed because there is a
   * discrepancy between how the attempt ID is constructed in the {@link
   * org.apache.tez.mapreduce.output} (with vertex ID appended to the end of the Job ID)
   * and how it's available in the mapper (without vertex ID) which creates and caches the
   * HiveIcebergRecordWriter object. 2. Redefine the equals/hashcode provided by TaskAttemptID so
   * that task type (map or reduce) does not count, and therefore the mapper and reducer threads can
   * use the same attempt ID-based key to retrieve the cached HiveIcebergRecordWriter object.
   */
  private static class TaskAttemptWrapper extends TaskAttemptID {

    TaskAttemptWrapper(TaskAttemptID attemptID, String vertexId) {
      super(
          getJobIDWithVertexAppended(attemptID.getJobID(), vertexId).getJtIdentifier(),
          attemptID.getJobID().getId(),
          attemptID.getTaskType(),
          attemptID.getTaskID().getId(),
          attemptID.getId());
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      TaskAttemptWrapper that = (TaskAttemptWrapper) o;
      return getId() == that.getId()
          && getTaskID().getId() == that.getTaskID().getId()
          && Objects.equals(getJobID(), that.getJobID());
    }

    @Override
    public int hashCode() {
      return Objects.hash(getId(), getTaskID().getId(), getJobID());
    }
  }
}
