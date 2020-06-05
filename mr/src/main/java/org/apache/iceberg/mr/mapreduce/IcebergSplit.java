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

package org.apache.iceberg.mr.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.hadoop.Util;
import org.apache.iceberg.mr.IcebergMRConfig;
import org.apache.iceberg.mr.SerializationUtil;

public class IcebergSplit extends InputSplit implements org.apache.hadoop.mapred.InputSplit, Writable {

  public static final String[] ANYWHERE = new String[]{"*"};

  private CombinedScanTask task;

  private transient String[] locations;
  private final transient Configuration conf;

  public IcebergSplit(Configuration conf, CombinedScanTask task) {
    this.conf = conf;
    this.task = task;
  }

  @Override
  public long getLength() {
    return task.files().stream().mapToLong(FileScanTask::length).sum();
  }

  @Override
  public String[] getLocations() {
    if (locations == null) {
      locations = IcebergMRConfig.localityPreferred(conf) ? Util.blockLocations(task, conf) : ANYWHERE;
    }

    return locations;
  }

  public CombinedScanTask getTask() {
    return task;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    byte[] data = new byte[in.readInt()];
    in.readFully(data);
    this.task = SerializationUtil.deserializeFromBytes(data);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    byte[] data = SerializationUtil.serializeToBytes(this.task);
    out.writeInt(data.length);
    out.write(data);
  }
}
