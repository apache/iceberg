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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mr.SerializationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CombineHiveInputFormat.AvoidSplitCombination is implemented to correctly delegate InputSplit
 * creation to this class. See: https://stackoverflow.com/questions/29133275/
 * custom-inputformat-getsplits-never-called-in-hive
 */
public class IcebergInputFormat<T> implements InputFormat<Void, T>, CombineHiveInputFormat.AvoidSplitCombination {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergInputFormat.class);

  static final String TABLE_LOCATION = "location";
  static final String REUSE_CONTAINERS = "iceberg.mr.reuse.containers";

  private Table table;

  @Override
  public InputSplit[] getSplits(JobConf conf, int numSplits) throws IOException {
    table = findTable(conf);
    CloseableIterable taskIterable = table.newScan().planTasks();
    List<CombinedScanTask> tasks = (List<CombinedScanTask>) StreamSupport
        .stream(taskIterable.spliterator(), false)
        .collect(Collectors.toList());
    return createSplits(tasks, table.location());
  }

  private Table findTable(JobConf conf) throws IOException {
    HadoopTables tables = new HadoopTables(conf);
    String tableDir = conf.get(TABLE_LOCATION);
    if (tableDir == null) {
      throw new IllegalArgumentException("Table 'location' not set in JobConf");
    }
    URI location = null;
    try {
      location = new URI(tableDir);
    } catch (URISyntaxException e) {
      throw new IOException("Unable to create URI for table location: '" + tableDir + "'", e);
    }
    table = tables.load(location.getPath());
    return table;
  }

  private InputSplit[] createSplits(List<CombinedScanTask> tasks, String location) {
    InputSplit[] splits = new InputSplit[tasks.size()];
    for (int i = 0; i < tasks.size(); i++) {
      splits[i] = new IcebergSplit(tasks.get(i), location);
    }
    return splits;
  }

  @Override
  public RecordReader getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {
    return new IcebergRecordReader(split, job);
  }

  @Override
  public boolean shouldSkipCombine(Path path, Configuration conf) throws IOException {
    return true;
  }

  public class IcebergRecordReader implements RecordReader<Void, IcebergWritable> {
    private JobConf conf;
    private IcebergSplit split;

    private Iterator<FileScanTask> tasks;
    private CloseableIterable<Record> reader;
    private Iterator<Record> recordIterator;
    private Record currentRecord;
    private boolean reuseContainers;

    public IcebergRecordReader(InputSplit split, JobConf conf) throws IOException {
      this.split = (IcebergSplit) split;
      this.conf = conf;
      this.reuseContainers = conf.getBoolean(REUSE_CONTAINERS, false);
      initialise();
    }

    private void initialise() {
      tasks = split.getTask().files().iterator();
      nextTask();
    }

    private void nextTask() {
      FileScanTask currentTask = tasks.next();
      DataFile file = currentTask.file();
      InputFile inputFile = HadoopInputFile.fromLocation(file.path(), conf);
      Schema tableSchema = table.schema();
      IcebergReaderFactory<Record> readerFactory = new IcebergReaderFactory<Record>();
      reader = readerFactory.createReader(file, currentTask, inputFile, tableSchema, reuseContainers);
      recordIterator = reader.iterator();
    }

    @Override
    public boolean next(Void key, IcebergWritable value) {
      if (recordIterator.hasNext()) {
        currentRecord = recordIterator.next();
        value.setRecord(currentRecord);
        return true;
      }

      if (tasks.hasNext()) {
        try {
          reader.close();
        } catch (IOException e) {
          LOG.error("Error closing reader", e);
        }
        nextTask();
        currentRecord = recordIterator.next();
        value.setRecord(currentRecord);
        return true;
      }
      return false;
    }

    @Override
    public Void createKey() {
      return null;
    }

    @Override
    public IcebergWritable createValue() {
      IcebergWritable record = new IcebergWritable();
      record.setRecord(currentRecord);
      record.setSchema(table.schema());
      return record;
    }

    @Override
    public long getPos() throws IOException {
      return 0;
    }

    @Override
    public void close() throws IOException {
      reader.close();
    }

    @Override
    public float getProgress() throws IOException {
      return 0;
    }
  }

  /**
   * FileSplit is extended rather than implementing the InputSplit interface due to Hive's HiveInputFormat
   * expecting a split which is an instance of FileSplit.
   */
  private static class IcebergSplit extends FileSplit {

    private static final String[] ANYWHERE = new String[]{"*"};

    private CombinedScanTask task;
    private String partitionLocation;

    IcebergSplit() {
    }

    IcebergSplit(CombinedScanTask task, String partitionLocation) {
      this.task = task;
      this.partitionLocation = partitionLocation;
    }

    @Override
    public long getLength() {
      return task.files().stream().mapToLong(FileScanTask::length).sum();
    }

    @Override
    public String[] getLocations() throws IOException {
      return ANYWHERE;
    }

    @Override
    public Path getPath() {
      return new Path(partitionLocation);
    }

    @Override
    public long getStart() {
      return 0L;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      byte[] data = SerializationUtil.serializeToBytes(this.task);
      out.writeInt(data.length);
      out.write(data);

      byte[] tableLocation = SerializationUtil.serializeToBytes(this.partitionLocation);
      out.writeInt(tableLocation.length);
      out.write(tableLocation);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      byte[] data = new byte[in.readInt()];
      in.readFully(data);
      this.task = SerializationUtil.deserializeFromBytes(data);

      byte[] location = new byte[in.readInt()];
      in.readFully(location);
      this.partitionLocation = SerializationUtil.deserializeFromBytes(location);
    }

    public CombinedScanTask getTask() {
      return task;
    }
  }

}
