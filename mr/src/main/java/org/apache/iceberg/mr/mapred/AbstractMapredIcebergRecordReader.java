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
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.iceberg.mr.mapreduce.IcebergSplit;

@SuppressWarnings("checkstyle:VisibilityModifier")
public abstract class AbstractMapredIcebergRecordReader<T> implements RecordReader<Void, T> {

  protected final org.apache.hadoop.mapreduce.RecordReader<Void, ?> innerReader;

  public AbstractMapredIcebergRecordReader(
      org.apache.iceberg.mr.mapreduce.IcebergInputFormat<?> mapreduceInputFormat,
      IcebergSplit split,
      JobConf job,
      Reporter reporter)
      throws IOException {
    TaskAttemptContext context = MapredIcebergInputFormat.newTaskAttemptContext(job, reporter);

    try {
      innerReader = mapreduceInputFormat.createRecordReader(split, context);
      innerReader.initialize(split, context);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  @Override
  public Void createKey() {
    return null;
  }

  @Override
  public float getProgress() throws IOException {
    try {
      return innerReader.getProgress();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() throws IOException {
    if (innerReader != null) {
      innerReader.close();
    }
  }
}
