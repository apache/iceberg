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

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.Reporter;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.mr.mapreduce.IcebergWritable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class HiveIcebergRecordWriter extends org.apache.hadoop.mapreduce.RecordWriter<NullWritable, IcebergWritable>
    implements FileSinkOperator.RecordWriter, org.apache.hadoop.mapred.RecordWriter<NullWritable, IcebergWritable> {
  private static final Logger LOG = LoggerFactory.getLogger(HiveIcebergRecordWriter.class);

  private final FileIO io;
  private final String location;
  private final FileFormat fileFormat;
  private final GenericAppenderFactory appenderFactory;
  // The current key is reused at every write to avoid unnecessary object creation
  private final PartitionKey currentKey;
  // Data for every partition is written to a different appender
  // This map stores the open appenders for the given partition key
  private final Map<PartitionKey, AppenderWrapper> openAppenders = new HashMap<>();
  // When the appenders are closed the file data needed for the Iceberg commit is stored and accessible through
  // this map
  private final Map<PartitionKey, ClosedFileData> closedFileData = new HashMap<>();

  HiveIcebergRecordWriter(Configuration conf, String location, FileFormat fileFormat, Schema schema,
      PartitionSpec spec) {
    this.io = new HadoopFileIO(conf);
    this.location = location;
    this.fileFormat = fileFormat;
    this.appenderFactory = new GenericAppenderFactory(schema);
    this.currentKey = new PartitionKey(spec, schema);
    LOG.info("IcebergRecordWriter is created in {} with {}", location, fileFormat);
  }

  @Override
  public void write(Writable row) {
    Preconditions.checkArgument(row instanceof IcebergWritable);

    Record record = ((IcebergWritable) row).record();

    currentKey.partition(record);

    AppenderWrapper currentAppender = openAppenders.get(currentKey);
    if (currentAppender == null) {
      currentAppender = getAppender();
      openAppenders.put(currentKey.copy(), currentAppender);
    }

    currentAppender.appender.add(record);
  }

  @Override
  public void write(NullWritable key, IcebergWritable value) {
    write(value);
  }

  @Override
  public void close(boolean abort) throws IOException {
    // Close the open appenders and store the closed file data
    for (PartitionKey key : openAppenders.keySet()) {
      AppenderWrapper wrapper = openAppenders.get(key);
      wrapper.close();
      closedFileData.put(key,
          new ClosedFileData(key, wrapper.location, fileFormat, wrapper.length(), wrapper.metrics()));
    }

    openAppenders.clear();

    // If abort then remove the unnecessary files
    if (abort) {
      Tasks.foreach(closedFileData.values().stream().map(ClosedFileData::fileName).iterator())
          .retry(3)
          .suppressFailureWhenFinished()
          .onFailure((file, exception) -> LOG.debug("Failed on to remove file {} on abort", file, exception))
          .run(io::deleteFile);
    }
  }

  @Override
  public void close(org.apache.hadoop.mapreduce.TaskAttemptContext context) throws IOException {
    close(false);
  }

  @Override
  public void close(Reporter reporter) throws IOException {
    close(false);
  }

  FileIO io() {
    return io;
  }

  FileFormat fileFormat() {
    return fileFormat;
  }

  Set<ClosedFileData> closedFileData() {
    return new HashSet<>(closedFileData.values());
  }

  private AppenderWrapper getAppender() {
    String dataFileLocation = location + UUID.randomUUID();
    OutputFile dataFile = io.newOutputFile(dataFileLocation);

    FileAppender<Record> appender = appenderFactory.newAppender(dataFile, fileFormat);

    return new AppenderWrapper(appender, dataFileLocation);
  }

  private static final class AppenderWrapper {
    private final FileAppender<Record> appender;
    private final String location;

    AppenderWrapper(FileAppender<Record> appender, String location) {
      this.appender = appender;
      this.location = location;
    }

    public long length() {
      return appender.length();
    }

    public Metrics metrics() {
      return appender.metrics();
    }

    public void close() throws IOException {
      appender.close();
    }
  }
}
