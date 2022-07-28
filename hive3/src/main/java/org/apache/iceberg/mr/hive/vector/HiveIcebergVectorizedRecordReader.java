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
package org.apache.iceberg.mr.hive.vector;

import java.io.IOException;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.iceberg.mr.mapred.AbstractMapredIcebergRecordReader;
import org.apache.iceberg.mr.mapreduce.IcebergSplit;

/**
 * Basically an MR1 API implementing wrapper for transferring VectorizedRowBatch's produced by
 * IcebergInputformat$IcebergRecordReader which relies on the MR2 API format.
 */
public final class HiveIcebergVectorizedRecordReader
    extends AbstractMapredIcebergRecordReader<VectorizedRowBatch> {

  private final JobConf job;

  public HiveIcebergVectorizedRecordReader(
      org.apache.iceberg.mr.mapreduce.IcebergInputFormat<VectorizedRowBatch> mapreduceInputFormat,
      IcebergSplit split,
      JobConf job,
      Reporter reporter)
      throws IOException {
    super(mapreduceInputFormat, split, job, reporter);
    this.job = job;
  }

  @Override
  public boolean next(Void key, VectorizedRowBatch value) throws IOException {
    try {
      if (innerReader.nextKeyValue()) {
        VectorizedRowBatch newBatch = (VectorizedRowBatch) innerReader.getCurrentValue();
        value.cols = newBatch.cols;
        value.endOfFile = newBatch.endOfFile;
        value.selectedInUse = newBatch.selectedInUse;
        value.size = newBatch.size;
        return true;
      } else {
        return false;
      }
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(ie);
    }
  }

  @Override
  public VectorizedRowBatch createValue() {
    return CompatibilityHiveVectorUtils.findMapWork(job)
        .getVectorizedRowBatchCtx()
        .createVectorizedRowBatch();
  }

  @Override
  public long getPos() {
    return -1;
  }
}
