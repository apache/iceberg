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
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.Util;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.util.SerializationUtil;

// Since this class extends `mapreduce.InputSplit and implements `mapred.InputSplit`, it can be
// returned by both MR v1
// and v2 file formats.
public class IcebergSplit extends InputSplit
    implements org.apache.hadoop.mapred.InputSplit, IcebergSplitContainer {

  private static final String[] ANYWHERE = new String[] {"*"};

  private Table table;
  private CombinedScanTask task;

  private transient String[] locations;
  private transient Configuration conf;

  // public no-argument constructor for deserialization
  public IcebergSplit() {}

  IcebergSplit(Table table, Configuration conf, CombinedScanTask task) {
    this.table = table;
    this.task = task;
    this.conf = conf;
  }

  public CombinedScanTask task() {
    return task;
  }

  @Override
  public IcebergSplit icebergSplit() {
    return this;
  }

  @Override
  public long getLength() {
    return task.files().stream().mapToLong(FileScanTask::length).sum();
  }

  @Override
  public String[] getLocations() {
    // The implementation of getLocations() is only meant to be used during split computation
    // getLocations() won't be accurate when called on worker nodes and will always return "*"
    if (locations == null && conf != null) {
      boolean localityPreferred = conf.getBoolean(InputFormatConfig.LOCALITY, false);
      locations = localityPreferred ? Util.blockLocations(task, conf) : ANYWHERE.clone();
    } else {
      locations = ANYWHERE.clone();
    }

    return locations;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    byte[] tableData = SerializationUtil.serializeToBytes(table);
    out.writeInt(tableData.length);
    out.write(tableData);

    byte[] data = SerializationUtil.serializeToBytes(this.task);
    out.writeInt(data.length);
    out.write(data);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    byte[] tableData = new byte[in.readInt()];
    in.readFully(tableData);
    this.table = SerializationUtil.deserializeFromBytes(tableData);

    byte[] data = new byte[in.readInt()];
    in.readFully(data);
    this.task = SerializationUtil.deserializeFromBytes(data);
  }

  public Table table() {
    return table;
  }
}
