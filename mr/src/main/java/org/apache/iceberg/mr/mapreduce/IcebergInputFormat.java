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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
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
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.avro.DataReader;
import org.apache.iceberg.data.orc.GenericOrcReader;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.hadoop.Util;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mr.SerializationUtil;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
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

  static final String AS_OF_TIMESTAMP = "iceberg.mr.as.of.time";
  static final String CASE_SENSITIVE = "iceberg.mr.case.sensitive";
  static final String FILTER_EXPRESSION = "iceberg.mr.filter.expression";
  static final String IN_MEMORY_DATA_MODEL = "iceberg.mr.in.memory.data.model";
  static final String READ_SCHEMA = "iceberg.mr.read.schema";
  static final String REUSE_CONTAINERS = "iceberg.mr.reuse.containers";
  static final String SNAPSHOT_ID = "iceberg.mr.snapshot.id";
  static final String SPLIT_SIZE = "iceberg.mr.split.size";
  static final String TABLE_PATH = "iceberg.mr.table.path";
  static final String TABLE_SCHEMA = "iceberg.mr.table.schema";
  static final String LOCALITY = "iceberg.mr.locality";
  static final String CATALOG = "iceberg.mr.catalog";
  static final String SKIP_RESIDUAL_FILTERING = "skip.residual.filtering";

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
  public static ConfigBuilder configure(Job job) {
    job.setInputFormatClass(IcebergInputFormat.class);
    return new ConfigBuilder(job.getConfiguration());
  }

  public static class ConfigBuilder {
    private final Configuration conf;

    public ConfigBuilder(Configuration conf) {
      this.conf = conf;
      // defaults
      conf.setEnum(IN_MEMORY_DATA_MODEL, InMemoryDataModel.GENERIC);
      conf.setBoolean(SKIP_RESIDUAL_FILTERING, false);
      conf.setBoolean(CASE_SENSITIVE, true);
      conf.setBoolean(REUSE_CONTAINERS, false);
      conf.setBoolean(LOCALITY, false);
    }

    public ConfigBuilder readFrom(String path) {
      conf.set(TABLE_PATH, path);
      Table table = findTable(conf);
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

    /**
     * If this API is called. The input splits
     * constructed will have host location information
     */
    public ConfigBuilder preferLocality() {
      conf.setBoolean(LOCALITY, true);
      return this;
    }

    public ConfigBuilder catalogFunc(Class<? extends Function<Configuration, Catalog>> catalogFuncClass) {
      conf.setClass(CATALOG, catalogFuncClass, Function.class);
      return this;
    }

    public ConfigBuilder useHiveRows() {
      conf.set(IN_MEMORY_DATA_MODEL, InMemoryDataModel.HIVE.name());
      return this;
    }

    public ConfigBuilder usePigTuples() {
      conf.set(IN_MEMORY_DATA_MODEL, InMemoryDataModel.PIG.name());
      return this;
    }

    /**
     * Compute platforms pass down filters to data sources. If the data source cannot apply some filters, or only
     * partially applies the filter, it will return the residual filter back. If the platform can correctly apply
     * the residual filters, then it should call this api. Otherwise the current api will throw an exception if the
     * passed in filter is not completely satisfied.
     */
    public ConfigBuilder skipResidualFiltering() {
      conf.setBoolean(SKIP_RESIDUAL_FILTERING, true);
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
    Table table = findTable(conf);
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
    long splitSize = conf.getLong(SPLIT_SIZE, 0);
    if (splitSize > 0) {
      scan = scan.option(TableProperties.SPLIT_SIZE, String.valueOf(splitSize));
    }
    String schemaStr = conf.get(READ_SCHEMA);
    if (schemaStr != null) {
      scan.project(SchemaParser.fromJson(schemaStr));
    }

    // TODO add a filter parser to get rid of Serialization
    Expression filter = SerializationUtil.deserializeFromBase64(conf.get(FILTER_EXPRESSION));
    if (filter != null) {
      scan = scan.filter(filter);
    }

    splits = Lists.newArrayList();
    boolean applyResidual = !conf.getBoolean(SKIP_RESIDUAL_FILTERING, false);
    InMemoryDataModel model = conf.getEnum(IN_MEMORY_DATA_MODEL, InMemoryDataModel.GENERIC);
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
    private boolean reuseContainers;
    private boolean caseSensitive;
    private InMemoryDataModel inMemoryDataModel;
    private Map<String, Integer> namesToPos;
    private Iterator<FileScanTask> tasks;
    private T currentRow;
    private CloseableIterator<T> currentIterator;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext newContext) {
      Configuration conf = newContext.getConfiguration();
      // For now IcebergInputFormat does its own split planning and does not accept FileSplit instances
      CombinedScanTask task = ((IcebergSplit) split).task;
      this.context = newContext;
      this.tasks = task.files().iterator();
      this.tableSchema = SchemaParser.fromJson(conf.get(TABLE_SCHEMA));
      String readSchemaStr = conf.get(READ_SCHEMA);
      this.expectedSchema = readSchemaStr != null ? SchemaParser.fromJson(readSchemaStr) : tableSchema;
      this.namesToPos = buildNameToPos(expectedSchema);
      this.reuseContainers = conf.getBoolean(REUSE_CONTAINERS, false);
      this.caseSensitive = conf.getBoolean(CASE_SENSITIVE, true);
      this.inMemoryDataModel = conf.getEnum(IN_MEMORY_DATA_MODEL, InMemoryDataModel.GENERIC);
      this.currentIterator = open(tasks.next());
    }

    @Override
    public boolean nextKeyValue() throws IOException {
      while (true) {
        if (currentIterator.hasNext()) {
          currentRow = currentIterator.next();
          return true;
        } else if (tasks.hasNext()) {
          currentIterator.close();
          currentIterator = open(tasks.next());
        } else {
          currentIterator.close();
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
      currentIterator.close();
    }

    private static Map<String, Integer> buildNameToPos(Schema expectedSchema) {
      Map<String, Integer> nameToPos = Maps.newHashMap();
      for (int pos = 0; pos < expectedSchema.asStruct().fields().size(); pos++) {
        Types.NestedField field = expectedSchema.asStruct().fields().get(pos);
        nameToPos.put(field.name(), pos);
      }
      return nameToPos;
    }

    private CloseableIterator<T> open(FileScanTask currentTask) {
      DataFile file = currentTask.file();
      // schema of rows returned by readers
      PartitionSpec spec = currentTask.spec();
      Set<Integer> idColumns =  Sets.intersection(spec.identitySourceIds(), TypeUtil.getProjectedIds(expectedSchema));
      boolean hasJoinedPartitionColumns = !idColumns.isEmpty();

      CloseableIterable<T> iterable;
      if (hasJoinedPartitionColumns) {
        Schema readDataSchema = TypeUtil.selectNot(expectedSchema, idColumns);
        Schema identityPartitionSchema = TypeUtil.select(expectedSchema, idColumns);
        iterable = CloseableIterable.transform(open(currentTask, readDataSchema),
            row -> withIdentityPartitionColumns(row, identityPartitionSchema, spec, file.partition()));
      } else {
        iterable = open(currentTask, expectedSchema);
      }

      return iterable.iterator();
    }

    private CloseableIterable<T> open(FileScanTask currentTask, Schema readSchema) {
      DataFile file = currentTask.file();
      // TODO we should make use of FileIO to create inputFile
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

      return iterable;
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

    private CloseableIterable<T> applyResidualFiltering(CloseableIterable<T> iter, Expression residual,
                                                        Schema readSchema) {
      boolean applyResidual = !context.getConfiguration().getBoolean(SKIP_RESIDUAL_FILTERING, false);

      if (applyResidual && residual != null && residual != Expressions.alwaysTrue()) {
        Evaluator filter = new Evaluator(readSchema.asStruct(), residual, caseSensitive);
        return CloseableIterable.filter(iter, record -> filter.eval((StructLike) record));
      } else {
        return iter;
      }
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
          throw new UnsupportedOperationException("Avro support not yet supported for Pig and Hive");
        case GENERIC:
          avroReadBuilder.createReaderFunc(DataReader::create);
      }
      return applyResidualFiltering(avroReadBuilder.build(), task.residual(), readSchema);
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
          throw new UnsupportedOperationException("Parquet support not yet supported for Pig and Hive");
        case GENERIC:
          parquetReadBuilder.createReaderFunc(
              fileSchema -> GenericParquetReaders.buildReader(readSchema, fileSchema));
      }
      return applyResidualFiltering(parquetReadBuilder.build(), task.residual(), readSchema);
    }

    private CloseableIterable<T> newOrcIterable(InputFile inputFile, FileScanTask task, Schema readSchema) {
      ORC.ReadBuilder orcReadBuilder = ORC.read(inputFile)
          .project(readSchema)
          .caseSensitive(caseSensitive)
          .split(task.start(), task.length());
      // ORC does not support reuse containers yet
      switch (inMemoryDataModel) {
        case PIG:
        case HIVE:
          //TODO: implement value readers for Pig and Hive
          throw new UnsupportedOperationException("ORC support not yet supported for Pig and Hive");
        case GENERIC:
          orcReadBuilder.createReaderFunc(fileSchema -> GenericOrcReader.buildReader(readSchema, fileSchema));
      }

      return applyResidualFiltering(orcReadBuilder.build(), task.residual(), readSchema);
    }
  }

  private static Table findTable(Configuration conf) {
    String path = conf.get(TABLE_PATH);
    Preconditions.checkArgument(path != null, "Table path should not be null");
    if (path.contains("/")) {
      HadoopTables tables = new HadoopTables(conf);
      return tables.load(path);
    }

    String catalogFuncClass = conf.get(CATALOG);
    if (catalogFuncClass != null) {
      Function<Configuration, Catalog> catalogFunc = (Function<Configuration, Catalog>)
          DynConstructors.builder(Function.class)
                         .impl(catalogFuncClass)
                         .build()
                         .newInstance();
      Catalog catalog = catalogFunc.apply(conf);
      TableIdentifier tableIdentifier = TableIdentifier.parse(path);
      return catalog.loadTable(tableIdentifier);
    } else {
      throw new IllegalArgumentException("No custom catalog specified to load table " + path);
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
      boolean localityPreferred = conf.getBoolean(LOCALITY, false);
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
