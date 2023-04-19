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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.iceberg.mr.mapreduce.IcebergSplit;
import org.apache.iceberg.mr.mapreduce.IcebergSplitContainer;
import org.apache.iceberg.util.SerializationUtil;

// Hive requires file formats to return splits that are instances of `FileSplit`.
public class HiveIcebergSplit extends FileSplit implements IcebergSplitContainer {

  private IcebergSplit innerSplit;

  // Hive uses the path name of a split to map it back to a partition (`PartitionDesc`) or table
  // description object
  // (`TableDesc`) which specifies the relevant input format for reading the files belonging to that
  // partition or table.
  // That way, `HiveInputFormat` and `CombineHiveInputFormat` can read files with different file
  // formats in the same
  // MapReduce job and merge compatible splits together.
  private String tableLocation;

  // public no-argument constructor for deserialization
  public HiveIcebergSplit() {}

  HiveIcebergSplit(IcebergSplit split, String tableLocation) {
    this.innerSplit = split;
    this.tableLocation = tableLocation;
  }

  @Override
  public IcebergSplit icebergSplit() {
    return innerSplit;
  }

  @Override
  public long getLength() {
    return innerSplit.getLength();
  }

  @Override
  public String[] getLocations() {
    return innerSplit.getLocations();
  }

  @Override
  public Path getPath() {
    return new Path(tableLocation);
  }

  @Override
  public long getStart() {
    return 0;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    byte[] bytes = SerializationUtil.serializeToBytes(tableLocation);
    out.writeInt(bytes.length);
    out.write(bytes);

    innerSplit.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    byte[] bytes = new byte[in.readInt()];
    in.readFully(bytes);
    tableLocation = SerializationUtil.deserializeFromBytes(bytes);

    innerSplit = new IcebergSplit();
    innerSplit.readFields(in);
  }
}
