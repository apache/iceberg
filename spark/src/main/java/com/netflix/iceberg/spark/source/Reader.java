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
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Closeables;
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
import com.netflix.iceberg.expressions.Evaluator;
import com.netflix.iceberg.expressions.Expression;
import com.netflix.iceberg.hadoop.HadoopInputFile;
import com.netflix.iceberg.io.InputFile;
import com.netflix.iceberg.parquet.Parquet;
import com.netflix.iceberg.spark.SparkFilters;
import com.netflix.iceberg.spark.SparkSchemaUtil;
import com.netflix.iceberg.spark.data.SparkAvroReader;
import com.netflix.iceberg.spark.data.SparkOrcReader;
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
import org.apache.spark.sql.execution.datasources.parquet.ParquetReadSupport;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.v2.reader.DataReader;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.sources.v2.reader.DataReaderFactory;
import org.apache.spark.sql.sources.v2.reader.Statistics;
import org.apache.spark.sql.sources.v2.reader.SupportsPushDownFilters;
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

class Reader implements DataSourceReader, SupportsScanUnsafeRow,
    SupportsPushDownRequiredColumns, SupportsPushDownFilters, SupportsReportStatistics {
  private static final Filter[] NO_FILTERS = new Filter[0];
  private static final List<String> SNAPSHOT_COLUMNS = ImmutableList.of(
      "snapshot_id", "file_path", "file_ordinal", "file_format", "block_size_in_bytes",
      "file_size_in_bytes", "record_count", "partition"
  );

  private final Table table;
  private final SerializableConfiguration conf;
  private StructType requestedSchema = null;
  private List<Expression> filterExpressions = null;
  private Filter[] pushedFilters = NO_FILTERS;

  // lazy variables
  private Schema schema = null;
  private StructType type = null; // cached because Spark accesses it multiple times
  private List<FileScanTask> tasks = null; // lazy cache of tasks

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
    for (FileScanTask fileTask : tasks()) {
      readTasks.add(new ScanTask(fileTask, tableSchemaString, expectedSchemaString, conf));
    }

    return readTasks;
  }

  @Override
  public Filter[] pushFilters(Filter[] filters) {
    // TODO: this needs to add filter columns to the projection
    this.tasks = null; // invalidate cached tasks, if present

    List<Expression> expressions = Lists.newArrayListWithExpectedSize(filters.length);
    List<Filter> pushed = Lists.newArrayListWithExpectedSize(filters.length);
    List<Filter> notSupported = Lists.newArrayListWithExpectedSize(0);

    for (Filter filter : filters) {
      Expression expr = SparkFilters.convert(filter);
      if (expr != null) {
        expressions.add(expr);
        pushed.add(filter);
      } else {
        notSupported.add(filter);
      }
    }

    this.filterExpressions = expressions;
    this.pushedFilters = pushed.toArray(new Filter[pushed.size()]);

    // invalidate the schema that will be projected
    this.schema = null;
    this.type = null;

    return notSupported.toArray(new Filter[notSupported.size()]);
  }

  @Override
  public Filter[] pushedFilters() {
    return pushedFilters;
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

    for (FileScanTask task : tasks) {
      sizeInBytes += task.length();
      numRows += task.file().recordCount();
    }

    return new Stats(sizeInBytes, numRows);
  }

  private List<FileScanTask> tasks() {
    if (tasks == null) {
      TableScan scan = table.newScan().select(SNAPSHOT_COLUMNS);

      if (filterExpressions != null) {
        for (Expression filter : filterExpressions) {
          scan = scan.filter(filter);
        }
      }

      boolean threw = true;
      try {
        this.tasks = Lists.newArrayList(scan.planFiles());
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

  private static class ScanTask implements DataReaderFactory<UnsafeRow>, Serializable {
    // for some reason, the apply method can't be called from Java without reflection
    private static final DynMethods.UnboundMethod APPLY_PROJECTION = DynMethods.builder("apply")
        .impl(UnsafeProjection.class, InternalRow.class)
        .build();

    private final FileScanTask task;
    private final String tableSchemaString;
    private final String expectedSchemaString;
    private final SerializableConfiguration conf;

    private transient Schema tableSchema = null;
    private transient Schema expectedSchema = null;

    private ScanTask(FileScanTask task, String tableSchemaString, String expectedSchemaString,
                     SerializableConfiguration conf) {
      this.task = task;
      this.tableSchemaString = tableSchemaString;
      this.expectedSchemaString = expectedSchemaString;
      this.conf = conf;
    }

    @Override
    public DataReader<UnsafeRow> createDataReader() {
      DataFile file = task.file();
      InputFile location = HadoopInputFile.fromLocation(file.path(), conf.value());

      // schema or rows returned by readers
      Schema finalSchema = lazyExpectedSchema();
      PartitionSpec spec = task.spec();
      Set<Integer> idColumns = identitySourceIds(spec);

      // schema needed for the projection and filtering
      Schema requiredSchema = prune(lazyTableSchema(), convert(finalSchema), task.residual());
      boolean hasJoinedPartitionColumns = !idColumns.isEmpty();
      boolean hasExtraFilterColumns = requiredSchema.columns().size() != finalSchema.columns().size();

      Iterator<UnsafeRow> unsafeRowIterator;
      switch (file.format()) {
        case PARQUET:
          if (hasJoinedPartitionColumns) {
            // schema used to read data files
            Schema readSchema = TypeUtil.selectNot(requiredSchema, idColumns);
            Schema partitionSchema = TypeUtil.select(requiredSchema, idColumns);
            Schema joinedSchema = TypeUtil.join(readSchema, partitionSchema);
            PartitionRowConverter convertToRow = new PartitionRowConverter(partitionSchema, spec);
            JoinedRow joined = new JoinedRow();

            InternalRow partition = convertToRow.apply(file.partition());
            joined.withRight(partition);

            // create joined rows and project from the joined schema to the final schema
            Iterator<InternalRow> joinedIter = transform(
                newParquetIterator(location, task, readSchema), joined::withLeft);

            unsafeRowIterator = transform(joinedIter,
                APPLY_PROJECTION.bind(projection(finalSchema, joinedSchema))::invoke);

          } else if (hasExtraFilterColumns) {
            // add projection to the final schema
            unsafeRowIterator = transform(newParquetIterator(location, task, requiredSchema),
                APPLY_PROJECTION.bind(projection(finalSchema, requiredSchema))::invoke);

          } else {
            // return the base iterator
            unsafeRowIterator = newParquetIterator(location, task, finalSchema);
          }
          break;

        case AVRO:
          Schema iterSchema;
          Iterator<InternalRow> iter;
          if (hasJoinedPartitionColumns) {
            Schema readSchema = TypeUtil.selectNot(requiredSchema, idColumns);
            Schema partitionSchema = TypeUtil.select(requiredSchema, idColumns);

            PartitionRowConverter convertToRow = new PartitionRowConverter(partitionSchema, spec);
            JoinedRow joined = new JoinedRow();
            InternalRow partition = convertToRow.apply(file.partition());
            joined.withRight(partition);

            // create joined rows and project from the joined schema to the final schema
            iterSchema = TypeUtil.join(readSchema, partitionSchema);
            iter = transform(newParquetIterator(location, task, readSchema), joined::withLeft);

          } else if (hasExtraFilterColumns) {
            // add projection to the final schema
            iterSchema = requiredSchema;
            iter = newAvroIterator(location, task, iterSchema);

          } else {
            // return the base iterator
            iterSchema = finalSchema;
            iter = newAvroIterator(location, task, iterSchema);
          }

          Expression residual = task.residual();
          if (residual.op() != Expression.Operation.TRUE) {
            Evaluator eval = new Evaluator(iterSchema.asStruct(), residual);
            StructLikeInternalRow wrapper = new StructLikeInternalRow(convert(iterSchema));
            iter = Iterators.filter(iter, input -> eval.eval(wrapper.setRow(input)));
          }

          unsafeRowIterator = transform(iter,
              APPLY_PROJECTION.bind(projection(finalSchema, iterSchema))::invoke);
          break;

        case ORC:
          unsafeRowIterator = new SparkOrcReader(location, task, finalSchema, conf);
          break;

        default:
          throw new UnsupportedOperationException("Cannot read unknown format: " + file.format());
      }

      return new IteratorReader(unsafeRowIterator);
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

    private UnsafeProjection projection(Schema finalSchema, Schema readSchema) {
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

    private Iterator<InternalRow> newAvroIterator(InputFile location,
                                                  FileScanTask task,
                                                  Schema readSchema) {
      return Avro.read(location)
          .reuseContainers()
          .project(readSchema)
          .split(task.start(), task.length())
          .createReaderFunc(SparkAvroReader::new)
          .<InternalRow>build()
          .iterator();
    }

    private <R extends InternalRow> Iterator<R> newParquetIterator(InputFile location,
                                                                   FileScanTask task,
                                                                   Schema readSchema) {
      StructType readType = convert(readSchema);
      return Parquet.read(location)
          .project(readSchema)
          .split(task.start(), task.length())
          .readSupport(new ParquetReadSupport())
          .filter(task.residual())
          .set("org.apache.spark.sql.parquet.row.requested_schema", readType.json())
          .set("spark.sql.parquet.binaryAsString", "false")
          .set("spark.sql.parquet.int96AsTimestamp", "false")
          .callInit()
          .<R>build()
          .iterator();
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

  private static class IteratorReader implements DataReader<UnsafeRow> {
    private final Iterator<UnsafeRow> rows;
    private final Closeable closeable;
    private UnsafeRow current = null;

    private IteratorReader(Iterator<UnsafeRow> rows) {
      this.rows = rows;
      this.closeable = (rows instanceof Closeable) ? (Closeable) rows : null;
    }

    @Override
    public boolean next() throws IOException {
      if (rows.hasNext()) {
        this.current = rows.next();
        return true;
      }
      return false;
    }

    @Override
    public UnsafeRow get() {
      return current;
    }

    @Override
    public void close() throws IOException {
      if (closeable != null) {
        closeable.close();
      }
    }
  }
}
