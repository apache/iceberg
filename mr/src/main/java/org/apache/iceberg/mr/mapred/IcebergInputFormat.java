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
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
import org.apache.hadoop.hive.ql.io.sarg.ConvertAstToSearchArg;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotsTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.mr.SerializationUtil;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CombineHiveInputFormat.AvoidSplitCombination is implemented to correctly delegate InputSplit
 * creation to this class. See: https://stackoverflow.com/questions/29133275/
 * custom-inputformat-getsplits-never-called-in-hive
 */
public class IcebergInputFormat<T> implements InputFormat<Void, T>, CombineHiveInputFormat.AvoidSplitCombination {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergInputFormat.class);

  private Table table;
  private long currentSnapshotId;
  private String virtualSnapshotIdColumnName;

  @Override
  public InputSplit[] getSplits(JobConf conf, int numSplits) throws IOException {
    table = TableResolver.resolveTableFromJob(conf);
    URI location = TableResolver.pathAsURI(conf.get(InputFormatConfig.TABLE_LOCATION));
    List<CombinedScanTask> tasks = planTasks(conf);
    return createSplits(tasks, location.toString());
  }

  private List<CombinedScanTask> planTasks(JobConf conf) {
    // Set defaults for virtual column
    Snapshot currentSnapshot = table.currentSnapshot();
    if (currentSnapshot != null) {
      currentSnapshotId = currentSnapshot.snapshotId();
    }
    virtualSnapshotIdColumnName = SystemTableUtil.getVirtualColumnName(conf);

    String[] readColumns = ColumnProjectionUtils.getReadColumnNames(conf);
    List<CombinedScanTask> tasks;
    if (conf.get(TableScanDesc.FILTER_EXPR_CONF_STR) == null) {
      tasks = Lists.newArrayList(table
              .newScan()
              .select(readColumns)
              .planTasks());
    } else {
      ExprNodeGenericFuncDesc exprNodeDesc = SerializationUtilities
              .deserializeObject(conf.get(TableScanDesc.FILTER_EXPR_CONF_STR), ExprNodeGenericFuncDesc.class);
      SearchArgument sarg = ConvertAstToSearchArg.create(conf, exprNodeDesc);
      Expression filter = IcebergFilterFactory.generateFilterExpression(sarg);

      long snapshotIdToScan = extractSnapshotID(conf, exprNodeDesc);

      tasks = Lists.newArrayList(table
              .newScan()
              .useSnapshot(snapshotIdToScan)
              .select(readColumns)
              .filter(filter)
              .planTasks());
    }
    return tasks;
  }

  /**
   * Search all the leaves of the expression for the 'snapshot_id' column and extract value.
   * If snapshot_id column not found, return current table snapshot ID.
   */
  private long extractSnapshotID(Configuration conf, ExprNodeGenericFuncDesc exprNodeDesc) {
    SearchArgument sarg = ConvertAstToSearchArg.create(conf, exprNodeDesc);
    List<PredicateLeaf> leaves = sarg.getLeaves();
    for (PredicateLeaf leaf : leaves) {
      if (leaf.getColumnName().equals(virtualSnapshotIdColumnName)) {
        currentSnapshotId = (long) leaf.getLiteral();
        return (long) leaf.getLiteral();
      }
    }
    currentSnapshotId = table.currentSnapshot().snapshotId();
    return table.currentSnapshot().snapshotId();
  }

  private InputSplit[] createSplits(List<CombinedScanTask> tasks, String name) {
    InputSplit[] splits = new InputSplit[tasks.size()];
    for (int i = 0; i < tasks.size(); i++) {
      splits[i] = new IcebergSplit(tasks.get(i), name);
    }
    return splits;
  }

  @Override
  public RecordReader<Void, T> getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {
    return new IcebergRecordReader(split, job);
  }

  @Override
  public boolean shouldSkipCombine(Path path, Configuration conf) throws IOException {
    return true;
  }

  public class IcebergRecordReader<T> implements RecordReader<Void, IcebergWritable> {
    private JobConf conf;
    private IcebergSplit split;

    private Iterator<FileScanTask> tasks;
    private CloseableIterable<Record> reader;
    private Iterator<Record> recordIterator;
    private Record currentRecord;

    public IcebergRecordReader(InputSplit split, JobConf conf) throws IOException {
      this.split = (IcebergSplit) split;
      this.conf = conf;
      initialise();
    }

    private void initialise() {
      tasks = split.getTask().files().iterator();
      nextTask();
    }

    private void nextTask() {
      FileScanTask currentTask = tasks.next();
      Schema tableSchema = table.schema();
      org.apache.iceberg.mr.IcebergRecordReader<Record> wrappedReader =
          new org.apache.iceberg.mr.IcebergRecordReader<Record>();
      reader = wrappedReader.createReader(conf, currentTask, tableSchema);
      recordIterator = reader.iterator();
    }

    private Record resolveAppropriateRecordForTableType() {
      if (table instanceof SnapshotsTable) {
        return currentRecord;
      } else {
        return SystemTableUtil.recordWithVirtualColumn(currentRecord, currentSnapshotId, table.schema(),
                virtualSnapshotIdColumnName);
      }
    }

    @Override
    public boolean next(Void key, IcebergWritable value) {
      if (recordIterator.hasNext()) {
        currentRecord = recordIterator.next();
        value.setRecord(resolveAppropriateRecordForTableType());
        return true;
      }

      if (tasks.hasNext()) {
        /*try {
          reader.close();
        } catch (IOException e) {
          LOG.error("Error closing reader", e);
        }*/
        nextTask();
        currentRecord = recordIterator.next();
        value.setRecord(resolveAppropriateRecordForTableType());
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
      if (table instanceof SnapshotsTable) {
        record.setSchema(table.schema());
      } else {
        record.setSchema(SystemTableUtil.schemaWithVirtualColumn(table.schema(), virtualSnapshotIdColumnName));
      }
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
      return new String[0];
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
      byte[] dataTask = SerializationUtil.serializeToBytes(this.task);
      out.writeInt(dataTask.length);
      out.write(dataTask);

      byte[] tableName = SerializationUtil.serializeToBytes(this.partitionLocation);
      out.writeInt(tableName.length);
      out.write(tableName);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      byte[] data = new byte[in.readInt()];
      in.readFully(data);
      this.task = SerializationUtil.deserializeFromBytes(data);

      byte[] name = new byte[in.readInt()];
      in.readFully(name);
      this.partitionLocation = SerializationUtil.deserializeFromBytes(name);
    }

    public CombinedScanTask getTask() {
      return task;
    }
  }

}
