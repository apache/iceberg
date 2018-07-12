/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.iceberg.spark.source;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Closeables;
import com.netflix.iceberg.CombinedScanTask;
import com.netflix.iceberg.DataFile;
import com.netflix.iceberg.FileScanTask;
import com.netflix.iceberg.PartitionField;
import com.netflix.iceberg.PartitionSpec;
import com.netflix.iceberg.Schema;
import com.netflix.iceberg.SchemaParser;
import com.netflix.iceberg.StructLike;
import com.netflix.iceberg.Table;
import com.netflix.iceberg.TableScan;
import com.netflix.iceberg.avro.Avro;
import com.netflix.iceberg.common.DynMethods;
import com.netflix.iceberg.exceptions.RuntimeIOException;
import com.netflix.iceberg.expressions.Expression;
import com.netflix.iceberg.hadoop.HadoopInputFile;
import com.netflix.iceberg.io.CloseableIterable;
import com.netflix.iceberg.io.InputFile;
import com.netflix.iceberg.parquet.Parquet;
import com.netflix.iceberg.spark.SparkExpressions;
import com.netflix.iceberg.spark.SparkSchemaUtil;
import com.netflix.iceberg.spark.data.SparkAvroReader;
import com.netflix.iceberg.spark.data.SparkOrcReader;
import com.netflix.iceberg.spark.data.SparkParquetReaders;
import com.netflix.iceberg.types.TypeUtil;
import com.netflix.iceberg.types.Types;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.expressions.JoinedRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.sources.v2.reader.DataReader;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.sources.v2.reader.DataReaderFactory;
import org.apache.spark.sql.sources.v2.reader.Statistics;
import org.apache.spark.sql.sources.v2.reader.SupportsPushDownCatalystFilters;
import org.apache.spark.sql.sources.v2.reader.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.sources.v2.reader.SupportsReportStatistics;
import org.apache.spark.sql.sources.v2.reader.SupportsScanUnsafeRow;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.apache.spark.util.SerializableConfiguration;
import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.collect.Iterators.transform;
import static com.netflix.iceberg.spark.SparkSchemaUtil.convert;
import static com.netflix.iceberg.spark.SparkSchemaUtil.prune;
import static scala.collection.JavaConverters.asScalaBufferConverter;
import static scala.collection.JavaConverters.seqAsJavaListConverter;

class Reader implements DataSourceReader, SupportsScanUnsafeRow, SupportsPushDownCatalystFilters,
    SupportsPushDownRequiredColumns, SupportsReportStatistics {

  private static final org.apache.spark.sql.catalyst.expressions.Expression[] NO_EXPRS =
      new org.apache.spark.sql.catalyst.expressions.Expression[0];

  private static final List<String> SNAPSHOT_COLUMNS = ImmutableList.of(
      "snapshot_id", "file_path", "file_ordinal", "file_format", "block_size_in_bytes",
      "file_size_in_bytes", "record_count", "partition", "value_counts", "null_value_counts",
      "lower_bounds", "upper_bounds"
  );

  private final Table table;
  private final SerializableConfiguration conf;
  private StructType requestedSchema = null;
  private List<Expression> filterExpressions = null;
  private org.apache.spark.sql.catalyst.expressions.Expression[] pushedExprs = NO_EXPRS;

  // lazy variables
  private Schema schema = null;
  private StructType type = null; // cached because Spark accesses it multiple times
  private List<CombinedScanTask> tasks = null; // lazy cache of tasks

  Reader(Table table, Configuration conf) {
    this.table = table;
    this.conf = new SerializableConfiguration(conf);
    this.schema = table.schema();
  }

  private Schema lazySchema() {
    if (schema == null) {
      if (requestedSchema != null) {
        this.schema = prune(table.schema(), requestedSchema);
      } else {
        this.schema = table.schema();
      }
    }
    return schema;
  }

  private StructType lazyType() {
    if (type == null) {
      this.type = convert(lazySchema());
    }
    return type;
  }

  @Override
  public StructType readSchema() {
    return lazyType();
  }

  @Override
  public List<DataReaderFactory<UnsafeRow>> createUnsafeRowReaderFactories() {
    String tableSchemaString = SchemaParser.toJson(table.schema());
    String expectedSchemaString = SchemaParser.toJson(lazySchema());

    List<DataReaderFactory<UnsafeRow>> readTasks = Lists.newArrayList();
    for (CombinedScanTask task : tasks()) {
      readTasks.add(new ReadTask(task, tableSchemaString, expectedSchemaString, conf));
    }

    return readTasks;
  }

  @Override
  public org.apache.spark.sql.catalyst.expressions.Expression[] pushCatalystFilters(
      org.apache.spark.sql.catalyst.expressions.Expression[] filters) {
    this.tasks = null; // invalidate cached tasks, if present

    List<Expression> expressions = Lists.newArrayListWithExpectedSize(filters.length);
    List<org.apache.spark.sql.catalyst.expressions.Expression> pushed =
        Lists.newArrayListWithExpectedSize(filters.length);

    for (org.apache.spark.sql.catalyst.expressions.Expression filter : filters) {
      Expression expr = SparkExpressions.convert(filter);
      if (expr != null) {
        expressions.add(expr);
        pushed.add(filter);
      }
    }

    this.filterExpressions = expressions;
    this.pushedExprs = pushed.toArray(new org.apache.spark.sql.catalyst.expressions.Expression[0]);

    // invalidate the schema that will be projected
    this.schema = null;
    this.type = null;

    // Spark doesn't support residuals per task, so return all filters
    // to get Spark to handle record-level filtering
    return filters;
  }

  @Override
  public org.apache.spark.sql.catalyst.expressions.Expression[] pushedCatalystFilters() {
    return pushedExprs;
  }

  @Override
  public void pruneColumns(StructType requestedSchema) {
    this.requestedSchema = requestedSchema;

    // invalidate the schema that will be projected
    this.schema = null;
    this.type = null;
  }

  @Override
  public Statistics getStatistics() {
    long sizeInBytes = 0L;
    long numRows = 0L;

    for (CombinedScanTask task : tasks) {
      for (FileScanTask file : task.files()) {
        sizeInBytes += file.length();
        numRows += file.file().recordCount();
      }
    }

    return new Stats(sizeInBytes, numRows);
  }

  private List<CombinedScanTask> tasks() {
    if (tasks == null) {
      TableScan scan = table.newScan().select(SNAPSHOT_COLUMNS);

      if (filterExpressions != null) {
        for (Expression filter : filterExpressions) {
          scan = scan.filter(filter);
        }
      }

      boolean threw = true;
      try {
        this.tasks = Lists.newArrayList(scan.planTasks());
        threw = false;
      } finally {
        try {
          Closeables.close(scan, threw);
        } catch (IOException e) {
          throw new RuntimeIOException(e, "Failed to close table scan: %s", scan);
        }
      }
    }

    return tasks;
  }

  @Override
  public String toString() {
    return String.format(
        "IcebergScan(table=%s, type=%s, filters=%s)",
        table, lazySchema().asStruct(), filterExpressions);
  }

  private static class ReadTask implements DataReaderFactory<UnsafeRow>, Serializable {
    private final CombinedScanTask task;
    private final String tableSchemaString;
    private final String expectedSchemaString;
    private final SerializableConfiguration conf;

    private transient Schema tableSchema = null;
    private transient Schema expectedSchema = null;

    private ReadTask(CombinedScanTask task, String tableSchemaString, String expectedSchemaString,
                     SerializableConfiguration conf) {
      this.task = task;
      this.tableSchemaString = tableSchemaString;
      this.expectedSchemaString = expectedSchemaString;
      this.conf = conf;
    }

    @Override
    public DataReader<UnsafeRow> createDataReader() {
      return new TaskDataReader(task, lazyTableSchema(), lazyExpectedSchema(), conf.value());
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

  private static class TaskDataReader implements DataReader<UnsafeRow> {
    // for some reason, the apply method can't be called from Java without reflection
    private static final DynMethods.UnboundMethod APPLY_PROJECTION = DynMethods.builder("apply")
        .impl(UnsafeProjection.class, InternalRow.class)
        .build();

    private final Iterator<FileScanTask> tasks;
    private final Schema tableSchema;
    private final Schema expectedSchema;
    private final Configuration conf;

    private Iterator<UnsafeRow> currentIterator = null;
    private Closeable currentCloseable = null;
    private UnsafeRow current = null;

    public TaskDataReader(CombinedScanTask task, Schema tableSchema, Schema expectedSchema, Configuration conf) {
      this.tasks = task.files().iterator();
      this.tableSchema = tableSchema;
      this.expectedSchema = expectedSchema;
      this.conf = conf;
      // open last because the schemas and conf must be set
      this.currentIterator = open(tasks.next());
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
    public UnsafeRow get() {
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

    private Iterator<UnsafeRow> open(FileScanTask task) {
      DataFile file = task.file();

      // schema or rows returned by readers
      Schema finalSchema = expectedSchema;
      PartitionSpec spec = task.spec();
      Set<Integer> idColumns = identitySourceIds(spec);

      // schema needed for the projection and filtering
      Schema requiredSchema = prune(tableSchema, convert(finalSchema), task.residual());
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
        iter = transform(open(task, readSchema, conf), joined::withLeft);

      } else if (hasExtraFilterColumns) {
        // add projection to the final schema
        iterSchema = requiredSchema;
        iter = open(task, requiredSchema, conf);

      } else {
        // return the base iterator
        iterSchema = finalSchema;
        iter = open(task, finalSchema, conf);
      }

      return transform(iter,
          APPLY_PROJECTION.bind(projection(finalSchema, iterSchema))::invoke);
    }

    private static UnsafeProjection projection(Schema finalSchema, Schema readSchema) {
      StructType struct = convert(readSchema);

      List<AttributeReference> refs = seqAsJavaListConverter(struct.toAttributes()).asJava();
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
          asScalaBufferConverter(exprs).asScala().toSeq(),
          asScalaBufferConverter(attrs).asScala().toSeq());
    }

    private static Set<Integer> identitySourceIds(PartitionSpec spec) {
      Set<Integer> sourceIds = Sets.newHashSet();
      List<PartitionField> fields = spec.fields();
      for (int i = 0; i < fields.size(); i += 1) {
        PartitionField field = fields.get(i);
        if ("identity".equals(field.transform().toString())) {
          sourceIds.add(field.sourceId());
        }
      }

      return sourceIds;
    }

    private Iterator<InternalRow> open(FileScanTask task, Schema readSchema,
                                       Configuration conf) {
      InputFile location = HadoopInputFile.fromLocation(task.file().path(), conf);
      CloseableIterable<InternalRow> iter;
      switch (task.file().format()) {
        case ORC:
          SparkOrcReader reader = new SparkOrcReader(location, task, readSchema);
          this.currentCloseable = reader;
          return reader;

        case PARQUET:
          iter = newParquetIterable(location, task, readSchema);
          break;

        case AVRO:
          iter = newAvroIterable(location, task, readSchema);
          break;

        default:
          throw new UnsupportedOperationException(
              "Cannot read unknown format: " + task.file().format());
      }

      this.currentCloseable = iter;

      return iter.iterator();
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
          .build();
    }
  }

  private static class PartitionRowConverter implements Function<StructLike, InternalRow> {
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
        reusedRow.update(i, convert(tuple.get(positions[i], javaTypes[i]), types[i]));
      }

      return reusedRow;
    }

    /**
     * Converts the objects into instances used by Spark's InternalRow.
     *
     * @param value a data value
     * @param type the Spark data type
     * @return the value converted to the representation expected by Spark's InternalRow.
     */
    private static Object convert(Object value, DataType type) {
      if (type instanceof StringType) {
        return UTF8String.fromString(value.toString());
      } else if (type instanceof BinaryType) {
        ByteBuffer buffer = (ByteBuffer) value;
        return buffer.get(new byte[buffer.remaining()]);
      } else if (type instanceof DecimalType) {
        return Decimal.fromDecimal(value);
      }
      return value;
    }
  }

  private static class StructLikeInternalRow implements StructLike {
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
    @SuppressWarnings("unchecked")
    public <T> T get(int pos, Class<T> javaClass) {
      return (T) row.get(pos, types[pos]);
    }

    @Override
    public <T> void set(int pos, T value) {
      throw new UnsupportedOperationException("Not implemented: set");
    }
  }
}
