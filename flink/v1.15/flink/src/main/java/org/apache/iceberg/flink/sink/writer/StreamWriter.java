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
package org.apache.iceberg.flink.sink.writer;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import org.apache.flink.api.connector.sink2.StatefulSink.StatefulSinkWriter;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.flink.sink.TaskWriterFactory;
import org.apache.iceberg.flink.sink.committer.FilesCommittable;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

public class StreamWriter
    implements StatefulSinkWriter<RowData, StreamWriterState>,
        TwoPhaseCommittingSink.PrecommittingSinkWriter<RowData, FilesCommittable> {
  private static final long serialVersionUID = 1L;

  private final String fullTableName;
  private final TaskWriterFactory<RowData> taskWriterFactory;
  private transient TaskWriter<RowData> writer;
  private transient int subTaskId;
  private final List<FilesCommittable> writeResultsState = Lists.newArrayList();

  public StreamWriter(
      String fullTableName,
      TaskWriterFactory<RowData> taskWriterFactory,
      int subTaskId,
      int numberOfParallelSubtasks) {
    this.fullTableName = fullTableName;
    this.subTaskId = subTaskId;

    this.taskWriterFactory = taskWriterFactory;
    // Initialize the task writer factory.
    taskWriterFactory.initialize(numberOfParallelSubtasks, subTaskId);
    // Initialize the task writer.
    this.writer = taskWriterFactory.create();
  }

  @Override
  public void write(RowData element, Context context) throws IOException, InterruptedException {
    writer.write(element);
  }

  @Override
  public void flush(boolean endOfInput) throws IOException {}

  @Override
  public void close() throws Exception {
    if (writer != null) {
      writer.close();
      writer = null;
    }
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("table_name", fullTableName)
        .add("subtask_id", subTaskId)
        .toString();
  }

  @Override
  public Collection<FilesCommittable> prepareCommit() throws IOException {
    writeResultsState.add(new FilesCommittable(writer.complete()));
    this.writer = taskWriterFactory.create();
    return writeResultsState;
  }

  @Override
  public List<StreamWriterState> snapshotState(long checkpointId) {
    List<StreamWriterState> state = Lists.newArrayList();
    state.add(new StreamWriterState(Lists.newArrayList(writeResultsState)));
    writeResultsState.clear();
    return state;
  }

  public StreamWriter initializeState(Collection<StreamWriterState> recoveredState) {
    for (StreamWriterState streamWriterState : recoveredState) {
      this.writeResultsState.addAll(streamWriterState.writeResults());
    }

    return this;
  }
}
