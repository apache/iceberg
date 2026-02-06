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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.mr.mapreduce.IcebergSplit;
import org.apache.iceberg.mr.mapreduce.IcebergSplitContainer;

/**
 * Generic MR v1 InputFormat API for Iceberg.
 *
 * @param <T> Java class of records constructed by Iceberg; default is {@link Record}
 */
public class MapredIcebergInputFormat<T> implements InputFormat<Void, Container<T>> {

  private final org.apache.iceberg.mr.mapreduce.IcebergInputFormat<T> innerInputFormat;

  public MapredIcebergInputFormat() {
    this.innerInputFormat = new org.apache.iceberg.mr.mapreduce.IcebergInputFormat<>();
  }

  /**
   * Configures the {@code JobConf} to use the {@code MapredIcebergInputFormat} and returns a helper
   * to add further configuration.
   *
   * @param job the {@code JobConf} to configure
   */
  public static InputFormatConfig.ConfigBuilder configure(JobConf job) {
    job.setInputFormat(MapredIcebergInputFormat.class);
    return new InputFormatConfig.ConfigBuilder(job);
  }

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    return innerInputFormat.getSplits(newJobContext(job)).stream()
        .map(InputSplit.class::cast)
        .toArray(InputSplit[]::new);
  }

  @Override
  public RecordReader<Void, Container<T>> getRecordReader(
      InputSplit split, JobConf job, Reporter reporter) throws IOException {
    IcebergSplit icebergSplit = ((IcebergSplitContainer) split).icebergSplit();
    return new MapredIcebergRecordReader<>(innerInputFormat, icebergSplit, job, reporter);
  }

  private static final class MapredIcebergRecordReader<T>
      extends AbstractMapredIcebergRecordReader<Container<T>> {

    private final long splitLength; // for getPos()

    MapredIcebergRecordReader(
        org.apache.iceberg.mr.mapreduce.IcebergInputFormat<T> mapreduceInputFormat,
        IcebergSplit split,
        JobConf job,
        Reporter reporter)
        throws IOException {
      super(mapreduceInputFormat, split, job, reporter);
      splitLength = split.getLength();
    }

    @Override
    public boolean next(Void key, Container<T> value) throws IOException {
      try {
        if (innerReader.nextKeyValue()) {
          value.set((T) innerReader.getCurrentValue());
          return true;
        }
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(ie);
      }

      return false;
    }

    @Override
    public Container<T> createValue() {
      return new Container<>();
    }

    @Override
    public long getPos() throws IOException {
      return (long) (splitLength * getProgress());
    }
  }

  private static JobContext newJobContext(JobConf job) {
    JobID jobID = Optional.ofNullable(JobID.forName(job.get(JobContext.ID))).orElseGet(JobID::new);

    return new JobContextImpl(job, jobID);
  }

  public static TaskAttemptContext newTaskAttemptContext(JobConf job, Reporter reporter) {
    TaskAttemptID taskAttemptID =
        Optional.ofNullable(TaskAttemptID.forName(job.get(JobContext.TASK_ATTEMPT_ID)))
            .orElseGet(TaskAttemptID::new);

    return new CompatibilityTaskAttemptContextImpl(job, taskAttemptID, reporter);
  }

  // Saving the Reporter instance here as it is required for Hive vectorized readers.
  public static class CompatibilityTaskAttemptContextImpl extends TaskAttemptContextImpl {

    private final Reporter legacyReporter;

    public CompatibilityTaskAttemptContextImpl(
        Configuration conf, TaskAttemptID taskId, Reporter reporter) {
      super(conf, taskId, toStatusReporter(reporter));
      this.legacyReporter = reporter;
    }

    public Reporter getLegacyReporter() {
      return legacyReporter;
    }
  }

  private static StatusReporter toStatusReporter(Reporter reporter) {
    return new StatusReporter() {
      @Override
      public Counter getCounter(Enum<?> name) {
        return reporter.getCounter(name);
      }

      @Override
      public Counter getCounter(String group, String name) {
        return reporter.getCounter(group, name);
      }

      @Override
      public void progress() {
        reporter.progress();
      }

      @Override
      public float getProgress() {
        return reporter.getProgress();
      }

      @Override
      public void setStatus(String status) {
        reporter.setStatus(status);
      }
    };
  }
}
