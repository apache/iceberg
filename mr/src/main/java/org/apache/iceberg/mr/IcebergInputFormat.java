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

package org.apache.iceberg.mr;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import java.io.Closeable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.common.DynClasses;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.hive.HiveCatalogs;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.TypeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class IcebergInputFormat<T> extends InputFormat<Void, T> {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergInputFormat.class);

  static final String FILTER_EXPRESSION = "iceberg.mr.filter.expression";
  static final String TABLE_SCHEMA = "iceberg.mr.table.schema";
  static final String TABLE_PATH = "iceberg.mr.table.path";
  static final String READ_SCHEMA = "iceberg.mr.read.schema";
  static final String READ_SUPPORT = "iceberg.mr.read.support";

  private transient Table table;
  private transient List<InputSplit> splits;

  public IcebergInputFormat() {
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) {
    if (splits != null) {
      LOG.info("Returning cached splits: {}", splits.size());
      return splits;
    }

    Configuration conf = context.getConfiguration();
    table = getTable(conf);
    TableScan scan = table.newScan();
    //TODO add caseSensitive, snapshot id etc..

    Expression filterExpression = SerializationUtil.deserializeFromBase64(conf.get(FILTER_EXPRESSION));
    if (filterExpression != null) {
      scan = scan.filter(filterExpression);
    }

    final String schemaStr = conf.get(READ_SCHEMA);
    if (schemaStr != null) {
      // Not sure if this is having any effect?
      scan.project(SchemaParser.fromJson(schemaStr));
    }

    splits = Lists.newArrayList();
    try (CloseableIterable<CombinedScanTask> tasksIterable = scan.planTasks()) {
      tasksIterable.forEach(task -> splits.add(new IcebergSplit(task)));
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to close table scan: %s", scan);
    }

    return splits;
  }

  @Override
  public RecordReader<Void, T> createRecordReader(InputSplit split, TaskAttemptContext context) {
    return new IcebergRecordReader();
  }

  public static ConfBuilder updateConf(
      Configuration conf, String path, Class<? extends ReadSupport<?>> readSupportClass) {
    return new ConfBuilder(conf, path, readSupportClass);
  }

  public static class ConfBuilder {
    private final Configuration conf;

    public ConfBuilder(Configuration conf, String path, Class<? extends ReadSupport<?>> readSupportClass) {
      this.conf = conf;
      conf.set(TABLE_PATH, path);
      conf.set(READ_SUPPORT, readSupportClass.getName());
      Table table = getTable(conf);
      conf.set(TABLE_SCHEMA, SchemaParser.toJson(table.schema()));
    }

    public ConfBuilder filterExpression(Expression expression) throws IOException {
      conf.set(FILTER_EXPRESSION, SerializationUtil.serializeToBase64(expression));
      return this;
    }

    public ConfBuilder project(Schema schema) {
      conf.set(READ_SCHEMA, SchemaParser.toJson(schema));
      return this;
    }

    //TODO: other options split-size, snapshotid etc..
    public Configuration updatedConf() {
      return conf;
    }
  }

  private static final class IcebergRecordReader<T> extends RecordReader<Void, T> {
    private TaskAttemptContext context;
    private Iterator<FileScanTask> tasks;
    private Iterator<T> currentIterator;
    private T currentRow;
    private Schema expectedSchema;
    private Schema tableSchema;
    private ReadSupport<T> readSupport;
    private Closeable currentCloseable;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) {
      Configuration conf = context.getConfiguration();
      CombinedScanTask task = ((IcebergSplit) split).task;
      this.context = context;
      this.tasks = task.files().iterator();
      this.tableSchema = SchemaParser.fromJson(conf.get(TABLE_SCHEMA));
      String readSchemaStr = conf.get(READ_SCHEMA);
      if (readSchemaStr != null) {
        this.expectedSchema = SchemaParser.fromJson(readSchemaStr);
      }
      this.readSupport = readSupport(conf);
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
      return context.getProgress();
    }

    @Override
    public void close() throws IOException {
      currentCloseable.close();
    }

    private ReadSupport<T> readSupport(Configuration conf) {
      String readSupportClassName = conf.get(READ_SUPPORT);
      try {
        return DynClasses
            .builder()
            .impl(readSupportClassName)
            .<ReadSupport<T>>buildChecked()
            .newInstance();
      } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
        throw new RuntimeException(String.format("Unable to instantiate read support %s", readSupportClassName), e);
      }
    }

    private Iterator<T> open(FileScanTask currentTask) {
      DataFile file = currentTask.file();
      // schema of rows returned by readers
      PartitionSpec spec = currentTask.spec();
      Set<Integer> idColumns = spec.identitySourceIds();

      boolean hasJoinedPartitionColumns = !idColumns.isEmpty();
      Schema readSchema = expectedSchema != null ? expectedSchema : tableSchema;
      if (hasJoinedPartitionColumns) {
        readSchema = TypeUtil.selectNot(tableSchema, idColumns);
        Schema partitionSchema = TypeUtil.select(tableSchema, idColumns);
        return Iterators.transform(
            open(currentTask, readSchema),
            row -> readSupport.withPartitionColumns(row, partitionSchema, spec, file.partition()));
      } else {
        return open(currentTask, readSchema);
      }
    }

    private Iterator<T> open(FileScanTask currentTask, Schema readSchema) {
      DataFile file = currentTask.file();
      // TODO should we somehow make use of FileIO to create inputFile?
      InputFile inputFile = HadoopInputFile.fromLocation(file.path(), context.getConfiguration());
      CloseableIterable<T> iterable;
      switch (file.format()) {
        case AVRO:
          iterable = newAvroIterable(inputFile, currentTask, readSchema);
          break;
        case ORC:
          iterable = newOrcIterable(inputFile, currentTask, readSchema);
          break;
        case PARQUET:
          iterable = newParquetIterable(inputFile, currentTask, readSchema);
          break;
        default:
          throw new UnsupportedOperationException(
              String.format("Cannot read %s file: %s", file.format().name(), file.path()));
      }
      currentCloseable = iterable;
      return iterable.iterator();
    }

    private CloseableIterable<T> newAvroIterable(InputFile inputFile, FileScanTask task, Schema readSchema) {
      Avro.ReadBuilder avroReadBuilder = Avro.read(inputFile)
                                             .createReaderFunc(readSupport.avroReadBiFunction())
                                             .createReaderFunc(readSupport.avroReadFunction())
                                             .project(readSchema)
                                             .split(task.start(), task.length());
      //.reuseContainers(reuseContainers);
      return avroReadBuilder.build();
    }


    private CloseableIterable<T> newParquetIterable(InputFile inputFile, FileScanTask task, Schema readSchema) {
      Parquet.ReadBuilder parquetReadBuilder = Parquet.read(inputFile)
                                                      .createBatchedReaderFunc(readSupport.parquetBatchReadFunction())
                                                      .createReaderFunc(readSupport.parquetReadFunction())
                                                      .createBatchedReaderFunc(readSupport.parquetBatchReadFunction())
                                                      .project(readSchema)
                                                      //.caseSensitive(caseSensitive)
                                                      .split(task.start(), task.length());
      return parquetReadBuilder.build();
    }

    private CloseableIterable<T> newOrcIterable(InputFile inputFile, FileScanTask task, Schema readSchema) {
      ORC.ReadBuilder orcReadBuilder = ORC.read(inputFile)
                                          .createReaderFunc(readSupport.orcReadFunction())
                                          .schema(readSchema)
                                          //.caseSensitive(caseSensitive)
                                          .split(task.start(), task.length());

      return orcReadBuilder.build();
    }
  }

  private static Table getTable(Configuration conf) {
    String path = conf.get(TABLE_PATH);
    if (path.contains("/")) {
      HadoopTables tables = new HadoopTables(conf);
      return tables.load(path);
    } else {
      Catalog catalog = HiveCatalogs.loadCatalog(conf);
      TableIdentifier tableIdentifier = TableIdentifier.parse(path);
      return catalog.loadTable(tableIdentifier);
    }
  }

  private static class IcebergSplit extends InputSplit implements Writable {
    private static final String[] ANYWHERE = new String[]{"*"};
    CombinedScanTask task;

    IcebergSplit(CombinedScanTask task) {
      this.task = task;
    }

    public IcebergSplit() {
    }

    @Override
    public long getLength() {
      return task.files().stream().mapToLong(FileScanTask::length).sum();
    }

    @Override
    public String[] getLocations() {
      //TODO: add locations for hdfs
      return ANYWHERE;
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
