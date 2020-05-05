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

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.io.Closeable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.Util;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.mr.SerializationUtil;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generic Mrv2 InputFormat API for Iceberg.
 * @param <T> T is the in memory data model which can either be Pig tuples, Hive rows. Default is Iceberg records
 */
public class IcebergInputFormat<T> extends InputFormat<Void, T> {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergInputFormat.class);

  private transient List<InputSplit> splits;

  private enum InMemoryDataModel {
    PIG,
    HIVE,
    GENERIC // Default data model is of Iceberg Generics
  }

  /**
   * Configures the {@code Job} to use the {@code IcebergInputFormat} and
   * returns a helper to add further configuration.
   *
   * @param job the {@code Job} to configure
   */
  public static InputFormatConfig.ConfigBuilder configure(Job job) {
    job.setInputFormatClass(IcebergInputFormat.class);
    return new InputFormatConfig.ConfigBuilder(job.getConfiguration());
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) {
    if (splits != null) {
      LOG.info("Returning cached splits: {}", splits.size());
      return splits;
    }

    Configuration conf = context.getConfiguration();
    Table table = InputFormatConfig.findTable(conf);
    TableScan scan = InputFormatConfig.createTableScan(conf, table);

    splits = Lists.newArrayList();
    boolean applyResidual = !conf.getBoolean(InputFormatConfig.SKIP_RESIDUAL_FILTERING, false);
    InMemoryDataModel model = conf.getEnum(InputFormatConfig.IN_MEMORY_DATA_MODEL, InMemoryDataModel.GENERIC);
    try (CloseableIterable<CombinedScanTask> tasksIterable = scan.planTasks()) {
      tasksIterable.forEach(task -> {
        if (applyResidual && (model == InMemoryDataModel.HIVE || model == InMemoryDataModel.PIG)) {
          //TODO: We do not support residual evaluation for HIVE and PIG in memory data model yet
          checkResiduals(task);
        }
        splits.add(new IcebergSplit(conf, task));
      });
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to close table scan: %s", scan);
    }

    return splits;
  }

  private static void checkResiduals(CombinedScanTask task) {
    task.files().forEach(fileScanTask -> {
      Expression residual = fileScanTask.residual();
      if (residual != null && !residual.equals(Expressions.alwaysTrue())) {
        throw new UnsupportedOperationException(
            String.format(
                "Filter expression %s is not completely satisfied. Additional rows " +
                    "can be returned not satisfied by the filter expression", residual));
      }
    });
  }

  @Override
  public RecordReader<Void, T> createRecordReader(InputSplit split, TaskAttemptContext context) {
    return new IcebergRecordReader<>();
  }

  private static final class IcebergRecordReader<T> extends RecordReader<Void, T> {
    private TaskAttemptContext context;
    private Schema tableSchema;
    private Schema expectedSchema;
    private InMemoryDataModel inMemoryDataModel;
    private Map<String, Integer> namesToPos;
    private Iterator<FileScanTask> tasks;
    private T currentRow;
    private Iterator<T> currentIterator;
    private Closeable currentCloseable;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext newContext) {
      Configuration conf = newContext.getConfiguration();
      // For now IcebergInputFormat does its own split planning and does not accept FileSplit instances
      CombinedScanTask task = ((IcebergSplit) split).task;
      this.context = newContext;
      this.tasks = task.files().iterator();
      this.tableSchema = SchemaParser.fromJson(conf.get(InputFormatConfig.TABLE_SCHEMA));
      String readSchemaStr = conf.get(InputFormatConfig.READ_SCHEMA);
      this.expectedSchema = readSchemaStr != null ? SchemaParser.fromJson(readSchemaStr) : tableSchema;
      this.namesToPos = buildNameToPos(expectedSchema);
      this.inMemoryDataModel = conf.getEnum(InputFormatConfig.IN_MEMORY_DATA_MODEL, InMemoryDataModel.GENERIC);
      this.currentIterator = open(tasks.next());
    }

    @Override
    public boolean nextKeyValue() throws IOException {
      while (true) {
        if (currentIterator.hasNext()) {
          currentRow = currentIterator.next();
          return true;
        } else if (tasks.hasNext()) {
          currentCloseable.close();
          currentIterator = open(tasks.next());
        } else {
          currentCloseable.close();
          return false;
        }
      }
    }

    @Override
    public Void getCurrentKey() {
      return null;
    }

    @Override
    public T getCurrentValue() {
      return currentRow;
    }

    @Override
    public float getProgress() {
      // TODO: We could give a more accurate progress based on records read from the file. Context.getProgress does not
      // have enough information to give an accurate progress value. This isn't that easy, since we don't know how much
      // of the input split has been processed and we are pushing filters into Parquet and ORC. But we do know when a
      // file is opened and could count the number of rows returned, so we can estimate. And we could also add a row
      // count to the readers so that we can get an accurate count of rows that have been either returned or filtered
      // out.
      return context.getProgress();
    }

    @Override
    public void close() throws IOException {
      currentCloseable.close();
    }

    private static Map<String, Integer> buildNameToPos(Schema expectedSchema) {
      Map<String, Integer> nameToPos = Maps.newHashMap();
      for (int pos = 0; pos < expectedSchema.asStruct().fields().size(); pos++) {
        Types.NestedField field = expectedSchema.asStruct().fields().get(pos);
        nameToPos.put(field.name(), pos);
      }
      return nameToPos;
    }

    private Iterator<T> open(FileScanTask currentTask) {
      DataFile file = currentTask.file();
      // schema of rows returned by readers
      PartitionSpec spec = currentTask.spec();
      Set<Integer> idColumns =  Sets.intersection(spec.identitySourceIds(), TypeUtil.getProjectedIds(expectedSchema));
      boolean hasJoinedPartitionColumns = !idColumns.isEmpty();

      if (hasJoinedPartitionColumns) {
        Schema readDataSchema = TypeUtil.selectNot(expectedSchema, idColumns);
        Schema identityPartitionSchema = TypeUtil.select(expectedSchema, idColumns);
        return Iterators.transform(
            open(currentTask, readDataSchema),
            row -> withIdentityPartitionColumns(row, identityPartitionSchema, spec, file.partition()));
      } else {
        return open(currentTask, expectedSchema);
      }
    }

    private Iterator<T> open(FileScanTask currentTask, Schema readSchema) {
      org.apache.iceberg.mr.IcebergRecordReader<T> wrappedReader = new org.apache.iceberg.mr.IcebergRecordReader<T>();
      CloseableIterable<T> iterable = wrappedReader.createReader(context.getConfiguration(), currentTask, readSchema);
      currentCloseable = iterable;
      return iterable.iterator();
    }

    @SuppressWarnings("unchecked")
    private T withIdentityPartitionColumns(
        T row, Schema identityPartitionSchema, PartitionSpec spec, StructLike partition) {
      switch (inMemoryDataModel) {
        case PIG:
        case HIVE:
          throw new UnsupportedOperationException(
              "Adding partition columns to Pig and Hive data model are not supported yet");
        case GENERIC:
          return (T) withIdentityPartitionColumns((Record) row, identityPartitionSchema, spec, partition);
      }
      return row;
    }

    private Record withIdentityPartitionColumns(
        Record record, Schema identityPartitionSchema, PartitionSpec spec, StructLike partitionTuple) {
      List<PartitionField> partitionFields = spec.fields();
      List<Types.NestedField> identityColumns = identityPartitionSchema.columns();
      GenericRecord row = GenericRecord.create(expectedSchema.asStruct());
      namesToPos.forEach((name, pos) -> {
        Object field = record.getField(name);
        if (field != null) {
          row.set(pos, field);
        }

        // if the current name, pos points to an identity partition column, we set the
        // column at pos correctly by reading the corresponding value from partitionTuple`
        for (int i = 0; i < identityColumns.size(); i++) {
          Types.NestedField identityColumn = identityColumns.get(i);
          for (int j = 0; j < partitionFields.size(); j++) {
            PartitionField partitionField = partitionFields.get(j);
            if (name.equals(identityColumn.name()) &&
                identityColumn.fieldId() == partitionField.sourceId() &&
                "identity".equals(partitionField.transform().toString())) {
              row.set(pos, partitionTuple.get(j, spec.javaClasses()[j]));
            }
          }
        }
      });

      return row;
    }

  }

  static class IcebergSplit extends InputSplit implements Writable {
    static final String[] ANYWHERE = new String[]{"*"};
    private CombinedScanTask task;
    private transient String[] locations;
    private transient Configuration conf;

    IcebergSplit(Configuration conf, CombinedScanTask task) {
      this.task = task;
      this.conf = conf;
    }

    @Override
    public long getLength() {
      return task.files().stream().mapToLong(FileScanTask::length).sum();
    }

    @Override
    public String[] getLocations() {
      boolean localityPreferred = conf.getBoolean(InputFormatConfig.LOCALITY, false);
      if (!localityPreferred) {
        return ANYWHERE;
      }
      if (locations != null) {
        return locations;
      }
      locations = Util.blockLocations(task, conf);
      return locations;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      byte[] data = SerializationUtil.serializeToBytes(this.task);
      out.writeInt(data.length);
      out.write(data);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      byte[] data = new byte[in.readInt()];
      in.readFully(data);
      this.task = SerializationUtil.deserializeFromBytes(data);
    }
  }
}
