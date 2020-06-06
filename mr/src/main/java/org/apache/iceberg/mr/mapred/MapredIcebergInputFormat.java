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

package org.apache.iceberg.mr.mapred;

import java.io.IOException;
import java.util.Optional;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TaskAttemptContextImpl;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.iceberg.mr.IcebergMRConfig;
import org.apache.iceberg.mr.mapreduce.IcebergSplit;

/**
 * Generic Mrv1 InputFormat API for Iceberg.
 *
 * @param <T> T is the in memory data model which can either be Pig tuples, Hive rows. Default is Iceberg records
 */
public class MapredIcebergInputFormat<T> implements InputFormat<Void, Container<T>> {

  private final org.apache.iceberg.mr.mapreduce.IcebergInputFormat<T> innerInputFormat;

  public MapredIcebergInputFormat() {
    this.innerInputFormat = new org.apache.iceberg.mr.mapreduce.IcebergInputFormat<>();
  }

  /**
   * Configures the {@code JobConf} to use the {@code MapredIcebergInputFormat} and
   * returns a helper to add further configuration.
   *
   * @param job the {@code JobConf} to configure
   */
  public static IcebergMRConfig.Builder configure(JobConf job) {
    job.setInputFormat(MapredIcebergInputFormat.class);
    return IcebergMRConfig.Builder.newInstance(job);
  }

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    return innerInputFormat.getSplits(getTaskAttemptContext(job))
            .stream()
            .map(InputSplit.class::cast)
            .toArray(InputSplit[]::new);
  }

  @Override
  public RecordReader<Void, Container<T>> getRecordReader(
          InputSplit split, JobConf job, Reporter reporter) throws IOException {
    return new MapredIcebergRecordReader<>(innerInputFormat, (IcebergSplit) split, job, reporter);
  }

  static TaskAttemptContext getTaskAttemptContext(JobConf job) {
    TaskAttemptID taskAttemptID = Optional.ofNullable(TaskAttemptID.forName(job.get("mapred.task.id")))
            .orElse(new TaskAttemptID());

    return new TaskAttemptContextImpl(job, taskAttemptID);
  }

}
