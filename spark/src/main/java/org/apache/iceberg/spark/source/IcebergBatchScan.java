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

package org.apache.iceberg.spark.source;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataTask;
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
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.spark.SparkFilters;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.data.SparkAvroReader;
import org.apache.iceberg.spark.data.SparkOrcReader;
import org.apache.iceberg.spark.data.SparkParquetReaders;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.expressions.JoinedRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.Statistics;
import org.apache.spark.sql.connector.read.SupportsPushDownFilters;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.connector.read.SupportsReportStatistics;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.apache.spark.unsafe.types.UTF8String;
import scala.collection.JavaConverters;

public class IcebergBatchScan implements Scan,
    Batch,
    SupportsPushDownFilters,
    SupportsPushDownRequiredColumns,
    SupportsReportStatistics {
  private static final Filter[] NO_FILTERS = new Filter[0];

  private final Table table;
  private final Long snapshotId;
  private final Long asOfTimestamp;
  private final Long splitSize;
  private final Integer splitLookback;
  private final Long splitOpenFileCost;
  private final FileIO fileIo;
  private final EncryptionManager encryptionManager;
  private final boolean caseSensitive;
  private StructType requestedSchema = null;
  private List<Expression> filterExpressions = null;
  private Filter[] pushedFilters = NO_FILTERS;

  // lazy variables
  private Schema schema = null;
  private StructType type = null; // cached because Spark accesses it multiple times
  private List<CombinedScanTask> tasks = null; // lazy cache of tasks

  public IcebergBatchScan(Table table, Boolean caseSensitive, CaseInsensitiveStringMap options) {
    this.table = table;
    this.snapshotId = options.containsKey("snapshot-id") ? options.getLong("snapshot-id", 0) : null;
    this.asOfTimestamp = options.containsKey("as-of-timestamp") ? options.getLong("as-of-timestamp", 0) : null;

    if (snapshotId != null && asOfTimestamp != null) {
      throw new IllegalArgumentException(
          "Cannot scan using both snapshot-id and as-of-timestamp to select the table snapshot");
    }

    // look for split behavior overrides in options
    this.splitSize = options.containsKey("split-size") ? options.getLong("split-size",
        TableProperties.SPLIT_SIZE_DEFAULT) : null;
    this.splitLookback = options.containsKey("lookback") ? options.getInt("lookback",
        TableProperties.SPLIT_LOOKBACK_DEFAULT) : null;
    this.splitOpenFileCost = options.containsKey("file-open-cost") ? options.getLong("file-open-cost",
        TableProperties.SPLIT_OPEN_FILE_COST_DEFAULT) : null;

    this.schema = table.schema();
    this.fileIo = table.io();
    this.encryptionManager = table.encryption();
    this.caseSensitive = caseSensitive;
  }

  public IcebergBatchScan(Table table, CaseInsensitiveStringMap options) {
    this(table, true, options);
  }

  public IcebergBatchScan(Table table, CaseInsensitiveStringMap options, StructType requestedSchema) {
    this(table, true, options);

    if (requestedSchema != null) {
      pruneColumns(requestedSchema);
    }
  }

  private List<CombinedScanTask> tasks() {
    if (tasks == null) {
      TableScan scan = table
          .newScan()
          .caseSensitive(caseSensitive)
          .project(lazySchema());

      if (snapshotId != null) {
        scan = scan.useSnapshot(snapshotId);
      }

      if (asOfTimestamp != null) {
        scan = scan.asOfTime(asOfTimestamp);
      }

      if (splitSize != null) {
        scan = scan.option(TableProperties.SPLIT_SIZE, splitSize.toString());
      }

      if (splitLookback != null) {
        scan = scan.option(TableProperties.SPLIT_LOOKBACK, splitLookback.toString());
      }

      if (splitOpenFileCost != null) {
        scan = scan.option(TableProperties.SPLIT_OPEN_FILE_COST, splitOpenFileCost.toString());
      }

      if (filterExpressions != null) {
        for (Expression filter : filterExpressions) {
          scan = scan.filter(filter);
        }
      }

      try (CloseableIterable<CombinedScanTask> tasksIterable = scan.planTasks()) {
        this.tasks = Lists.newArrayList(tasksIterable);
      } catch (IOException e) {
        throw new RuntimeIOException(e, "Failed to close table scan: %s", scan);
      }
    }

    return tasks;
  }

  @Override
  public Filter[] pushFilters(Filter[] filters) {
    this.tasks = null; // invalidate cached tasks, if present

    List<Expression> expressions = Lists.newArrayListWithExpectedSize(filters.length);
    List<Filter> pushed = Lists.newArrayListWithExpectedSize(filters.length);

    for (Filter filter : filters) {
      Expression expr = SparkFilters.convert(filter);
      if (expr != null) {
        expressions.add(expr);
        pushed.add(filter);
      }
    }

    this.filterExpressions = expressions;
    this.pushedFilters = pushed.toArray(new Filter[0]);

    // invalidate the schema that will be projected
    this.schema = null;
    this.type = null;

    // Spark doesn't support residuals per task, so return all filters
    // to get Spark to handle record-level filtering
    return filters;
  }

  @Override
  public Batch toBatch() {
    return this;
  }

  @Override
  public Filter[] pushedFilters() {
    return pushedFilters;
  }

  @Override
  public void pruneColumns(StructType newRequestedSchema) {
    this.requestedSchema = newRequestedSchema;

    // invalidate the schema that will be projected
    this.schema = null;
    this.type = null;
  }

  @Override
  public Scan build() {
    return this;
  }

  @Override
  public Statistics estimateStatistics() {
    long sizeInBytes = 0L;
    long numRows = 0L;

    for (CombinedScanTask task : tasks()) {
      for (FileScanTask file : task.files()) {
        sizeInBytes += file.length();
        numRows += file.file().recordCount();
      }
    }

    return new Stats(sizeInBytes, numRows);
  }

  public static class BatchReadInputPartition implements InputPartition, Serializable {
    private final CombinedScanTask task;
    private final String tableSchemaString;
    private final String expectedSchemaString;
    private final FileIO fileIo;
    private final EncryptionManager encryptionManager;
    private final boolean caseSensitive;

    private transient Schema tableSchema = null;
    private transient Schema expectedSchema = null;

    BatchReadInputPartition(
        CombinedScanTask task,
        String tableSchemaString,
        String expectedSchemaString,
        FileIO fileIo,
        EncryptionManager encryptionManager,
        boolean caseSensitive) {
      this.task = task;
      this.tableSchemaString = tableSchemaString;
      this.expectedSchemaString = expectedSchemaString;
      this.fileIo = fileIo;
      this.encryptionManager = encryptionManager;
      this.caseSensitive = caseSensitive;
    }

    private Schema lazyTableSchema() {
      if (tableSchema == null) {
        this.tableSchema = SchemaParser.fromJson(tableSchemaString);
      }
      return tableSchema;
    }

    private Schema lazyExpectedSchema() {
      if (expectedSchema == null) {
        this.expectedSchema = SchemaParser.fromJson(expectedSchemaString);
      }
      return expectedSchema;
    }
  }

  @Override
  public InputPartition[] planInputPartitions() {
    String tableSchemaString = SchemaParser.toJson(table.schema());
    String expectedSchemaString = SchemaParser.toJson(lazySchema());

    List<CombinedScanTask> scanTasks = tasks();
    InputPartition[] readTasks = new InputPartition[scanTasks.size()];
    for (int i = 0; i < scanTasks.size(); i++) {
      readTasks[i] = new BatchReadInputPartition(scanTasks.get(i), tableSchemaString, expectedSchemaString, fileIo,
          encryptionManager, caseSensitive);
    }

    return readTasks;
  }

  @Override
  public IcebergRowReaderFactory createReaderFactory() {
    return new IcebergRowReaderFactory();
  }

  private Schema lazySchema() {
    if (schema == null) {
      if (requestedSchema != null) {
        this.schema = SparkSchemaUtil.prune(table.schema(), requestedSchema);
      } else {
        this.schema = table.schema();
      }
    }
    return schema;
  }

  private StructType lazyType() {
    if (type == null) {
      this.type = SparkSchemaUtil.convert(lazySchema());
    }
    return type;
  }

  @Override
  public StructType readSchema() {
    return lazyType();
  }


  public static class PartitionRowConverter implements Function<StructLike, InternalRow> {
    private final DataType[] types;
    private final int[] positions;
    private final Class<?>[] javaTypes;
    private final GenericInternalRow reusedRow;

    PartitionRowConverter(Schema partitionSchema, PartitionSpec spec) {
      StructType partitionType = SparkSchemaUtil.convert(partitionSchema);
      StructField[] fields = partitionType.fields();

      this.types = new DataType[fields.length];
      this.positions = new int[types.length];
      this.javaTypes = new Class<?>[types.length];
      this.reusedRow = new GenericInternalRow(types.length);

      List<PartitionField> partitionFields = spec.fields();
      for (int rowIndex = 0; rowIndex < fields.length; rowIndex += 1) {
        this.types[rowIndex] = fields[rowIndex].dataType();

        int sourceId = partitionSchema.columns().get(rowIndex).fieldId();
        for (int specIndex = 0; specIndex < partitionFields.size(); specIndex += 1) {
          PartitionField field = spec.fields().get(specIndex);
          if (field.sourceId() == sourceId && "identity".equals(field.transform().toString())) {
            positions[rowIndex] = specIndex;
            javaTypes[rowIndex] = spec.javaClasses()[specIndex];
            break;
          }
        }
      }
    }

    @Override
    public InternalRow apply(StructLike tuple) {
      for (int i = 0; i < types.length; i += 1) {
        Object value = tuple.get(positions[i], javaTypes[i]);
        if (value != null) {
          reusedRow.update(i, convert(value, types[i]));
        } else {
          reusedRow.setNullAt(i);
        }
      }

      return reusedRow;
    }

    /**
     * Converts the objects into instances used by Spark's InternalRow.
     *
     * @param value a data value
     * @param type  the Spark data type
     * @return the value converted to the representation expected by Spark's InternalRow.
     */
    private static Object convert(Object value, DataType type) {
      if (type instanceof StringType) {
        return UTF8String.fromString(value.toString());
      } else if (type instanceof BinaryType) {
        return ByteBuffers.toByteArray((ByteBuffer) value);
      } else if (type instanceof DecimalType) {
        return Decimal.fromDecimal(value);
      }
      return value;
    }
  }

  public static class StructLikeInternalRow implements StructLike {
    private final DataType[] types;
    private InternalRow row = null;

    StructLikeInternalRow(StructType struct) {
      this.types = new DataType[struct.size()];
      StructField[] fields = struct.fields();
      for (int i = 0; i < fields.length; i += 1) {
        types[i] = fields[i].dataType();
      }
    }

    public StructLikeInternalRow setRow(InternalRow row) {
      this.row = row;
      return this;
    }

    @Override
    public int size() {
      return types.length;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T get(int pos, Class<T> javaClass) {
      return javaClass.cast(row.get(pos, types[pos]));
    }

    @Override
    public <T> void set(int pos, T value) {
      throw new UnsupportedOperationException("Not implemented: set");
    }
  }


  private static class IcebergRowReaderFactory implements PartitionReaderFactory {
    IcebergRowReaderFactory() {
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition inputPartition) {
      return new TaskDataReader(inputPartition);
    }
  }

  public static class TaskDataReader implements PartitionReader<InternalRow> {
    // for some reason, the apply method can't be called from Java without reflection
    private static final DynMethods.UnboundMethod APPLY_PROJECTION = DynMethods.builder("apply")
        .impl(UnsafeProjection.class, InternalRow.class)
        .build();

    private final Iterator<FileScanTask> tasks;
    private final Schema tableSchema;
    private final Schema expectedSchema;
    private final FileIO fileIo;
    private final Map<String, InputFile> inputFiles;
    private final boolean caseSensitive;

    private Iterator<InternalRow> currentIterator = null;
    private Closeable currentCloseable = null;
    private InternalRow current = null;

    TaskDataReader(InputPartition inputPartition) {
      BatchReadInputPartition batchReadInputPartition = (BatchReadInputPartition) inputPartition;

      this.fileIo = batchReadInputPartition.fileIo;
      this.tableSchema = batchReadInputPartition.lazyTableSchema();
      this.expectedSchema = batchReadInputPartition.lazyExpectedSchema();
      this.tasks = batchReadInputPartition.task.files().iterator();
      Iterable<InputFile> decryptedFiles = batchReadInputPartition.encryptionManager.decrypt(
          Iterables.transform(batchReadInputPartition.task.files(),
              fileScanTask ->
                  EncryptedFiles.encryptedInput(
                      this.fileIo.newInputFile(fileScanTask.file().path().toString()),
                      fileScanTask.file().keyMetadata())));
      ImmutableMap.Builder<String, InputFile> inputFileBuilder = ImmutableMap.builder();
      decryptedFiles.forEach(decrypted -> inputFileBuilder.put(decrypted.location(), decrypted));
      this.inputFiles = inputFileBuilder.build();
      // open last because the schemas and fileIo must be set
      this.currentIterator = open(tasks.next());
      this.caseSensitive = batchReadInputPartition.caseSensitive;
    }

    @Override
    public boolean next() throws IOException {
      while (true) {
        if (currentIterator.hasNext()) {
          this.current = currentIterator.next();
          return true;

        } else if (tasks.hasNext()) {
          this.currentCloseable.close();
          this.currentIterator = open(tasks.next());

        } else {
          return false;
        }
      }
    }

    @Override
    public InternalRow get() {
      return current;
    }

    @Override
    public void close() throws IOException {
      // close the current iterator
      this.currentCloseable.close();

      // exhaust the task iterator
      while (tasks.hasNext()) {
        tasks.next();
      }
    }

    private Iterator<InternalRow> open(FileScanTask task) {
      DataFile file = task.file();

      // schema or rows returned by readers
      Schema finalSchema = expectedSchema;
      PartitionSpec spec = task.spec();
      Set<Integer> idColumns = spec.identitySourceIds();

      // schema needed for the projection and filtering
      StructType sparkType = SparkSchemaUtil.convert(finalSchema);
      Schema requiredSchema = SparkSchemaUtil.prune(tableSchema, sparkType, task.residual(), caseSensitive);
      boolean hasJoinedPartitionColumns = !idColumns.isEmpty();
      boolean hasExtraFilterColumns = requiredSchema.columns().size() != finalSchema.columns().size();

      Schema iterSchema;
      Iterator<InternalRow> iter;

      if (hasJoinedPartitionColumns) {
        // schema used to read data files
        Schema readSchema = TypeUtil.selectNot(requiredSchema, idColumns);
        Schema partitionSchema = TypeUtil.select(requiredSchema, idColumns);
        PartitionRowConverter convertToRow = new PartitionRowConverter(partitionSchema, spec);
        JoinedRow joined = new JoinedRow();

        InternalRow partition = convertToRow.apply(file.partition());
        joined.withRight(partition);

        // create joined rows and project from the joined schema to the final schema
        iterSchema = TypeUtil.join(readSchema, partitionSchema);
        iter = Iterators.transform(open(task, readSchema), joined::withLeft);

      } else if (hasExtraFilterColumns) {
        // add projection to the final schema
        iterSchema = requiredSchema;
        iter = open(task, requiredSchema);

      } else {
        // return the base iterator
        iterSchema = finalSchema;
        iter = open(task, finalSchema);
      }

      // TODO: remove the projection by reporting the iterator's schema back to Spark
      return Iterators.transform(iter,
          APPLY_PROJECTION.bind(projection(finalSchema, iterSchema))::invoke);
    }

    private Iterator<InternalRow> open(FileScanTask task, Schema readSchema) {
      CloseableIterable<InternalRow> iter;
      if (task.isDataTask()) {
        iter = newDataIterable(task.asDataTask(), readSchema);

      } else {
        InputFile location = inputFiles.get(task.file().path().toString());
        Preconditions.checkNotNull(location, "Could not find InputFile associated with FileScanTask");

        switch (task.file().format()) {
          case PARQUET:
            iter = newParquetIterable(location, task, readSchema);
            break;

          case AVRO:
            iter = newAvroIterable(location, task, readSchema);
            break;

          case ORC:
            iter = newOrcIterable(location, task, readSchema);
            break;

          default:
            throw new UnsupportedOperationException(
                "Cannot read unknown format: " + task.file().format());
        }
      }

      this.currentCloseable = iter;

      return iter.iterator();
    }

    private static UnsafeProjection projection(Schema finalSchema, Schema readSchema) {
      StructType struct = SparkSchemaUtil.convert(readSchema);

      List<AttributeReference> refs = JavaConverters.seqAsJavaListConverter(struct.toAttributes()).asJava();
      List<Attribute> attrs = Lists.newArrayListWithExpectedSize(struct.fields().length);
      List<org.apache.spark.sql.catalyst.expressions.Expression> exprs =
          Lists.newArrayListWithExpectedSize(struct.fields().length);

      for (AttributeReference ref : refs) {
        attrs.add(ref.toAttribute());
      }

      for (Types.NestedField field : finalSchema.columns()) {
        int indexInReadSchema = struct.fieldIndex(field.name());
        exprs.add(refs.get(indexInReadSchema));
      }

      return UnsafeProjection.create(
          JavaConverters.asScalaBufferConverter(exprs).asScala().toSeq(),
          JavaConverters.asScalaBufferConverter(attrs).asScala().toSeq());
    }

    private CloseableIterable<InternalRow> newAvroIterable(InputFile location,
                                                      FileScanTask task,
                                                      Schema readSchema) {
      return Avro.read(location)
          .reuseContainers()
          .project(readSchema)
          .split(task.start(), task.length())
          .createReaderFunc(SparkAvroReader::new)
          .build();
    }

    private CloseableIterable<InternalRow> newParquetIterable(InputFile location,
                                                            FileScanTask task,
                                                            Schema readSchema) {
      return Parquet.read(location)
          .project(readSchema)
          .split(task.start(), task.length())
          .createReaderFunc(fileSchema -> SparkParquetReaders.buildReader(readSchema, fileSchema))
          .filter(task.residual())
          .caseSensitive(caseSensitive)
          .build();
    }

    private CloseableIterable<InternalRow> newOrcIterable(InputFile location,
                                                          FileScanTask task,
                                                          Schema readSchema) {
      return ORC.read(location)
          .schema(readSchema)
          .split(task.start(), task.length())
          .createReaderFunc(SparkOrcReader::new)
          .caseSensitive(caseSensitive)
          .build();
    }

    private CloseableIterable<InternalRow> newDataIterable(DataTask task, Schema readSchema) {
      StructInternalRow row = new StructInternalRow(tableSchema.asStruct());
      CloseableIterable<InternalRow> asSparkRows = CloseableIterable.transform(
          task.asDataTask().rows(), row::setStruct);
      return CloseableIterable.transform(
          asSparkRows, APPLY_PROJECTION.bind(projection(readSchema, tableSchema))::invoke);
    }
  }

}
