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
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.avro.DataReader;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
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
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class IcebergInputFormat<T> extends InputFormat<Void, T> {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergInputFormat.class);

  static final String AS_OF_TIMESTAMP = "iceberg.mr.as.of.time";
  static final String CASE_SENSITIVE = "iceberg.mr.case.sensitive";
  static final String FILTER_EXPRESSION = "iceberg.mr.filter.expression";
  static final String IN_MEMORY_DATA_MODEL = "iceberg.mr.in.memory.data.model";
  static final String READ_SCHEMA = "iceberg.mr.read.schema";
  static final String REUSE_CONTAINERS = "iceberg.mr.case.sensitive";
  static final String SNAPSHOT_ID = "iceberg.mr.snapshot.id";
  static final String SPLIT_SIZE = "iceberg.mr.split.size";
  static final String TABLE_PATH = "iceberg.mr.table.path";
  static final String TABLE_SCHEMA = "iceberg.mr.table.schema";

  private transient List<InputSplit> splits;

  public enum InMemoryDataModel {
    PIG,
    HIVE,
    DEFAULT
  }

  /**
   * Configures the {@code Job} to use the {@code IcebergInputFormat} and
   * returns a helper to add further configuration.
   *
   * @param job the {@code Job} to configure
   */
  public static ConfigBuilder configure(Job job) {
    job.setInputFormatClass(IcebergInputFormat.class);
    return new ConfigBuilder(job.getConfiguration());
  }

  public static class ConfigBuilder {
    private final Configuration conf;

    public ConfigBuilder(Configuration conf) {
      this.conf = conf;
    }

    public ConfigBuilder readFrom(String path) {
      conf.set(TABLE_PATH, path);
      Table table = getTable(conf);
      conf.set(TABLE_SCHEMA, SchemaParser.toJson(table.schema()));
      return this;
    }

    public ConfigBuilder filter(Expression expression) {
      conf.set(FILTER_EXPRESSION, SerializationUtil.serializeToBase64(expression));
      return this;
    }

    public ConfigBuilder project(Schema schema) {
      conf.set(READ_SCHEMA, SchemaParser.toJson(schema));
      return this;
    }

    public ConfigBuilder reuseContainers(boolean reuse) {
      conf.setBoolean(REUSE_CONTAINERS, reuse);
      return this;
    }

    public ConfigBuilder caseSensitive(boolean caseSensitive) {
      conf.setBoolean(CASE_SENSITIVE, caseSensitive);
      return this;
    }

    public ConfigBuilder snapshotId(long snapshotId) {
      conf.setLong(SNAPSHOT_ID, snapshotId);
      return this;
    }

    public ConfigBuilder asOfTime(long asOfTime) {
      conf.setLong(AS_OF_TIMESTAMP, asOfTime);
      return this;
    }

    public ConfigBuilder splitSize(long splitSize) {
      conf.setLong(SPLIT_SIZE, splitSize);
      return this;
    }

    public ConfigBuilder inMemoryDataModel(InMemoryDataModel inMemoryDataModel) {
      conf.set(IN_MEMORY_DATA_MODEL, inMemoryDataModel.name());
      return this;
    }
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) {
    if (splits != null) {
      LOG.info("Returning cached splits: {}", splits.size());
      return splits;
    }

    Configuration conf = context.getConfiguration();
    Table table = getTable(conf);
    TableScan scan = table.newScan()
                          .caseSensitive(conf.getBoolean(CASE_SENSITIVE, true));

    long snapshotId = conf.getLong(SNAPSHOT_ID, -1);
    if (snapshotId != -1) {
      scan = scan.useSnapshot(snapshotId);
    }
    long asOfTime = conf.getLong(AS_OF_TIMESTAMP, -1);
    if (asOfTime != -1) {
      scan = scan.asOfTime(asOfTime);
    }
    long splitSize = conf.getLong(SPLIT_SIZE, -1);
    if (splitSize != -1) {
      scan = scan.option(TableProperties.SPLIT_SIZE, String.valueOf(splitSize));
    }
    String schemaStr = conf.get(READ_SCHEMA);
    if (schemaStr != null) {
      scan.project(SchemaParser.fromJson(schemaStr));
    }

    // TODO add a filter parser to get rid of Serialization
    Expression filterExpression = SerializationUtil.deserializeFromBase64(conf.get(FILTER_EXPRESSION));
    if (filterExpression != null) {
      scan = scan.filter(filterExpression);
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
    return new IcebergRecordReader<>();
  }

  private static final class IcebergRecordReader<T> extends RecordReader<Void, T> {
    private TaskAttemptContext context;
    private Iterator<FileScanTask> tasks;
    private Iterator<T> currentIterator;
    private T currentRow;
    private Schema expectedSchema;
    private Schema tableSchema;
    private InMemoryDataModel inMemoryDataModel;
    private Closeable currentCloseable;
    private boolean reuseContainers;
    private boolean caseSensitive;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext newContext) {
      Configuration conf = newContext.getConfiguration();
      CombinedScanTask task = ((IcebergSplit) split).task;
      this.context = newContext;
      this.tasks = task.files().iterator();
      this.tableSchema = SchemaParser.fromJson(conf.get(TABLE_SCHEMA));
      String readSchemaStr = conf.get(READ_SCHEMA);
      if (readSchemaStr != null) {
        this.expectedSchema = SchemaParser.fromJson(readSchemaStr);
      }
      this.reuseContainers = conf.getBoolean(REUSE_CONTAINERS, false);
      this.caseSensitive = conf.getBoolean(CASE_SENSITIVE, true);
      this.inMemoryDataModel = conf.getEnum(IN_MEMORY_DATA_MODEL, InMemoryDataModel.DEFAULT);
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

    private Iterator<T> open(FileScanTask currentTask) {
      DataFile file = currentTask.file();
      // schema of rows returned by readers
      PartitionSpec spec = currentTask.spec();
      Set<Integer> idColumns = spec.identitySourceIds();

      boolean hasJoinedPartitionColumns = !idColumns.isEmpty();
      Schema readSchema = expectedSchema != null ? expectedSchema : tableSchema;
      if (hasJoinedPartitionColumns) {
        readSchema = TypeUtil.selectNot(tableSchema, idColumns);
        Schema identityPartitionSchema = TypeUtil.select(tableSchema, idColumns);
        return Iterators.transform(
            open(currentTask, readSchema),
            row -> withPartitionColumns(row, identityPartitionSchema, spec, file.partition()));
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

    @SuppressWarnings("unchecked")
    private T withPartitionColumns(T row, Schema identityPartitionSchema, PartitionSpec spec, StructLike partition) {
      switch (inMemoryDataModel) {
        case PIG:
        case HIVE:
          // TODO implement adding partition columns to records for Pig and Hive
          throw new UnsupportedOperationException();
        case DEFAULT:
          return (T) icebergRecordWithPartitionsColumns((Record) row, identityPartitionSchema, spec, partition);
      }
      return row;
    }

    private static Record icebergRecordWithPartitionsColumns(
        Record record, Schema identityPartitionSchema, PartitionSpec spec, StructLike partition) {
      List<Types.NestedField> fields = Lists.newArrayList(record.struct().fields());
      fields.addAll(identityPartitionSchema.asStruct().fields());
      GenericRecord row = GenericRecord.create(Types.StructType.of(fields));
      int size = record.struct().fields().size();
      for (int i = 0; i < size; i++) {
        row.set(i, record.get(i));
      }
      List<PartitionField> partitionFields = spec.fields();
      List<Types.NestedField> identityColumns = identityPartitionSchema.columns();
      for (int i = 0; i < identityColumns.size(); i++) {
        Types.NestedField identityColumn = identityColumns.get(i);

        for (int j = 0; j < partitionFields.size(); j++) {
          PartitionField partitionField = partitionFields.get(j);
          if (identityColumn.fieldId() == partitionField.sourceId() &&
              "identity".equals(partitionField.transform().toString())) {
            row.set(size + i, partition.get(j, spec.javaClasses()[i]));
          } else {
            row.set(size + i, null);
          }
        }
      }
      return row;
    }

    private CloseableIterable<T> newAvroIterable(InputFile inputFile, FileScanTask task, Schema readSchema) {
      Avro.ReadBuilder avroReadBuilder = Avro.read(inputFile)
                                             .project(readSchema)
                                             .split(task.start(), task.length());

      if (reuseContainers) {
        avroReadBuilder.reuseContainers();
      }

      switch (inMemoryDataModel) {
        case PIG:
        case HIVE:
          //TODO implement value readers for Pig and Hive
          throw new UnsupportedOperationException();
        case DEFAULT:
          avroReadBuilder.createReaderFunc(DataReader::create);
      }
      return avroReadBuilder.build();
    }

    private CloseableIterable<T> newParquetIterable(InputFile inputFile, FileScanTask task, Schema readSchema) {
      Parquet.ReadBuilder parquetReadBuilder = Parquet.read(inputFile)
                                                      .project(readSchema)
                                                      .filter(task.residual())
                                                      .caseSensitive(caseSensitive)
                                                      .split(task.start(), task.length());
      if (reuseContainers) {
        parquetReadBuilder.reuseContainers();
      }

      switch (inMemoryDataModel) {
        case PIG:
        case HIVE:
          //TODO implement value readers for Pig and Hive
          throw new UnsupportedOperationException();
        case DEFAULT:
          parquetReadBuilder.createReaderFunc(
              fileSchema -> GenericParquetReaders.buildReader(readSchema, fileSchema));
      }
      return parquetReadBuilder.build();
    }

    private CloseableIterable<T> newOrcIterable(InputFile inputFile, FileScanTask task, Schema readSchema) {
      ORC.ReadBuilder orcReadBuilder = ORC.read(inputFile)
                                          .schema(readSchema)
                                          .caseSensitive(caseSensitive)
                                          .split(task.start(), task.length());
      // ORC does not support reuse containers yet
      switch (inMemoryDataModel) {
        case PIG:
        case HIVE:
          //TODO implement value readers for Pig and Hive
          throw new UnsupportedOperationException();
        case DEFAULT:
          //TODO: We do not have support for Iceberg generics for ORC
          throw new UnsupportedOperationException();
      }

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
    private CombinedScanTask task;

    IcebergSplit(CombinedScanTask task) {
      this.task = task;
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
