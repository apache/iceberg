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

import java.util.List;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;
import org.apache.iceberg.flink.maintenance.api.TaskResult;
import org.apache.iceberg.flink.maintenance.api.Trigger;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Aggregates results of the operators for a given maintenance task.
 *
 * <ul>
 *   <li>Input 1 is used:
 *       <ul>
 *         <li>To provide the {@link TaskResult#startEpoch()} - should be chained to the task input
 *         <li>To mark that the task is finished - should be chained at the end of the task, so an
 *             incoming watermark will signal that the task is finished
 *       </ul>
 *   <li>Input 2 expects an {@link Exception} which caused the failure - should be chained to the
 *       {@link #ERROR_STREAM} of the operators
 * </ul>
 *
 * The operator emits a {@link TaskResult} with the overall result on {@link Watermark}.
 */
@Internal
public class TaskResultAggregator extends AbstractStreamOperator<TaskResult>
    implements TwoInputStreamOperator<Trigger, Exception, TaskResult> {
  public static final OutputTag<Exception> ERROR_STREAM =
      new OutputTag<>("error-stream", TypeInformation.of(Exception.class));

  private static final Logger LOG = LoggerFactory.getLogger(TaskResultAggregator.class);

  private final String tableName;
  private final String taskName;
  private final int taskIndex;
  private final List<Exception> exceptions;
  private transient long startTime;

  public TaskResultAggregator(String tableName, String taskName, int taskIndex) {
    Preconditions.checkNotNull(tableName, "Table name should no be null");
    Preconditions.checkNotNull(taskName, "Task name should no be null");

    this.tableName = tableName;
    this.taskName = taskName;
    this.taskIndex = taskIndex;
    this.exceptions = Lists.newArrayList();
  }

  @Override
  public void processElement1(StreamRecord<Trigger> streamRecord) {
    startTime = streamRecord.getValue().timestamp();
  }

  @Override
  public void processElement2(StreamRecord<Exception> streamRecord) {
    Preconditions.checkNotNull(streamRecord.getValue(), "Exception could not be `null`.");
    exceptions.add(streamRecord.getValue());
  }

  @Override
  public void processWatermark(Watermark mark) throws Exception {
    if (startTime != 0L) {
      TaskResult response = new TaskResult(taskIndex, startTime, exceptions.isEmpty(), exceptions);
      output.collect(new StreamRecord<>(response));
      LOG.info(
          "Aggregated result for table {}, task {}[{}] is {}",
          tableName,
          taskName,
          taskIndex,
          response);
      exceptions.clear();
      startTime = 0L;
    }

    super.processWatermark(mark);
  }
}
