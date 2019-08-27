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

import com.google.common.collect.Lists;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.SystemProperties;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.spark.SparkFilters;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.apache.spark.sql.sources.v2.reader.Statistics;
import org.apache.spark.sql.sources.v2.reader.SupportsPushDownFilters;
import org.apache.spark.sql.sources.v2.reader.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.sources.v2.reader.SupportsReportStatistics;
import org.apache.spark.sql.sources.v2.reader.SupportsScanColumnarBatch;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Seq;
import scala.collection.mutable.ArrayBuffer;

class V1VectorizedReader implements SupportsScanColumnarBatch,
    DataSourceReader,
    SupportsPushDownFilters,
    SupportsPushDownRequiredColumns,
    SupportsReportStatistics {
  private static final Logger LOG = LoggerFactory.getLogger(Reader.class);

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
  private final int numRecordsPerBatch;
  // private final String sparkMaster;
  private final Configuration hadoopConf;
  // private final SparkConf sparkConf;
  private final SparkSession sparkSession;
  // default as per SQLConf.PARQUET_VECTORIZED_READER_BATCH_SIZE default
  public static final int DEFAULT_NUM_ROWS_IN_BATCH = 4096;

  private StructType requestedSchema = null;
  private List<Expression> filterExpressions = null;
  private Filter[] pushedFilters = NO_FILTERS;

  // lazy variables
  private Schema schema = null;
  private StructType type = null; // cached because Spark accesses it multiple times
  private List<CombinedScanTask> tasks = null; // lazy cache of tasks

  V1VectorizedReader(Table table, boolean caseSensitive, DataSourceOptions options,
      Configuration hadoopConf, int numRecordsPerBatch, SparkSession sparkSession) {

    this.table = table;
    this.snapshotId = options.get("snapshot-id").map(Long::parseLong).orElse(null);
    this.asOfTimestamp = options.get("as-of-timestamp").map(Long::parseLong).orElse(null);
    if (snapshotId != null && asOfTimestamp != null) {
      throw new IllegalArgumentException(
          "Cannot scan using both snapshot-id and as-of-timestamp to select the table snapshot");
    }

    this.numRecordsPerBatch = numRecordsPerBatch;


    this.splitSize = options.get("split-size").map(Long::parseLong).orElse(null);
    this.splitLookback = options.get("lookback").map(Integer::parseInt).orElse(null);
    this.splitOpenFileCost = options.get("file-open-cost").map(Long::parseLong).orElse(null);

    this.schema = table.schema();
    this.fileIo = table.io();
    this.encryptionManager = table.encryption();
    this.caseSensitive = caseSensitive;
    this.hadoopConf = hadoopConf;
    this.sparkSession = sparkSession;

    LOG.warn("=> Set Config numRecordsPerBatch: {}, " +
            "Split size: {}, " +
            "Planning Thread count: {}",
        numRecordsPerBatch, splitSize, System.getProperty(SystemProperties.WORKER_THREAD_POOL_SIZE_PROP));
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

  @Override
  public List<InputPartition<ColumnarBatch>> planBatchInputPartitions() {

    long start = System.currentTimeMillis();

    String tableSchemaString = SchemaParser.toJson(table.schema());
    String expectedSchemaString = SchemaParser.toJson(lazySchema());

    List<InputPartition<ColumnarBatch>> readTasks = Lists.newArrayList();
    for (CombinedScanTask task : tasks()) {
      readTasks.add(
          new ReadTask(task, tableSchemaString, expectedSchemaString, fileIo, encryptionManager, caseSensitive,
              numRecordsPerBatch, hadoopConf, pushFilters(pushedFilters), sparkSession));
    }
    LOG.warn("=> Input Task planning took {} seconds.", (System.currentTimeMillis() - start) / 1000.0f);

    return readTasks;
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
      }  catch (IOException e) {
        throw new RuntimeIOException(e, "Failed to close table scan: %s", scan);
      }
    }

    return tasks;
  }

  @Override
  public String toString() {
    return String.format(
        "V1VectorizedIcebergScan(numPerBatch=%s, table=%s, type=%s, filters=%s, caseSensitive=%s)",
        numRecordsPerBatch, table, lazySchema().asStruct(), filterExpressions, caseSensitive);
  }

  private static class ReadTask implements InputPartition<ColumnarBatch>, Serializable {
    private final CombinedScanTask task;
    private final String tableSchemaString;
    private final String expectedSchemaString;
    private final FileIO fileIo;
    private final EncryptionManager encryptionManager;
    private final boolean caseSensitive;
    private final int numRecordsPerBatch;
    private final Filter[] filters;
    private final ParquetFileFormat fileFormatInstance;
    private final scala.Function1<PartitionedFile, scala.collection.Iterator<InternalRow>> buildReaderFunc;

    private transient Schema tableSchema = null;
    private transient Schema expectedSchema = null;

    private ReadTask(
        CombinedScanTask task, String tableSchemaString, String expectedSchemaString, FileIO fileIo,
        EncryptionManager encryptionManager, boolean caseSensitive, int numRecordsPerBatch,
        Configuration hadoopConf, Filter[] filters, SparkSession sparkSession) {
      this.task = task;
      this.tableSchemaString = tableSchemaString;
      this.expectedSchemaString = expectedSchemaString;
      this.fileIo = fileIo;
      this.encryptionManager = encryptionManager;
      this.caseSensitive = caseSensitive;
      this.numRecordsPerBatch = numRecordsPerBatch;
      this.filters = filters;

      // Build function to for V1 Partition Reader which is passed over from Driver to Executors
      this.fileFormatInstance = new ParquetFileFormat();

      scala.collection.mutable.ArrayBuffer<Filter> filtersAsArrayBuf =  new ArrayBuffer(filters.length);
      for (Filter f : filters) {
        filtersAsArrayBuf.$plus$eq(f);
      }
      Seq<Filter> filterAsSeq = filtersAsArrayBuf.toSeq();

      // Seq<Filter> filtersAsSeq = JavaConverters.collectionAsScalaIterableConverter(filtersAsList).asScala().toSeq();
      // Seq<Filter> filtersAsSeq = JavaConverters.asScalaIteratorConverter(filtersAsList.iterator()).asScala().toSeq();
      StructType sparkReadSchema = SparkSchemaUtil.convert(lazyExpectedSchema());

      hadoopConf.set(SQLConf.PARQUET_VECTORIZED_READER_ENABLED().key(), "true");
      hadoopConf.set(SQLConf.PARQUET_VECTORIZED_READER_BATCH_SIZE().key(),
          Integer.toString(this.numRecordsPerBatch));
      sparkSession.sessionState().conf().setConfString(SQLConf.PARQUET_VECTORIZED_READER_ENABLED().key(), "true");
      sparkSession.sessionState().conf().setConfString(SQLConf.PARQUET_VECTORIZED_READER_BATCH_SIZE().key(),
          Integer.toString(this.numRecordsPerBatch));

      LOG.warn("=> Build partition reader function ");
      this.buildReaderFunc = fileFormatInstance.buildReaderWithPartitionValues(sparkSession,
          sparkReadSchema,
          new StructType(),
          sparkReadSchema,
          filterAsSeq, // List$.MODULE$.empty(),
          null, hadoopConf);
    }

    @Override
    public InputPartitionReader<ColumnarBatch> createPartitionReader() {

      LOG.warn("=> Create Partition Reader");
      return new V1VectorizedTaskDataReader(task, lazyTableSchema(), lazyExpectedSchema(), fileIo,
            encryptionManager, caseSensitive, numRecordsPerBatch, filters, buildReaderFunc);
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
     * @param type the Spark data type
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

  private static class SerializableHadoopConfiguration implements Serializable {

    private Configuration conf;

    SerializableHadoopConfiguration(Configuration hadoopConf) {
      this.conf = hadoopConf;

      if (this.conf == null) {
        this.conf = new Configuration();
      }
    }

    SerializableHadoopConfiguration() {
      this.conf = new Configuration();
    }

    public Configuration get() {
      return this.conf;
    }

    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
      this.conf.write(out);
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException {
      this.conf = new Configuration();
      this.conf.readFields(in);
    }
  }

}
