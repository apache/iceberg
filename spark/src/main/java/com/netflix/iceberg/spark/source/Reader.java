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
import com.netflix.iceberg.DataFile;
import com.netflix.iceberg.FileScanTask;
import com.netflix.iceberg.Schema;
import com.netflix.iceberg.SchemaParser;
import com.netflix.iceberg.Table;
import com.netflix.iceberg.TableScan;
import com.netflix.iceberg.avro.Avro;
import com.netflix.iceberg.common.DynMethods;
import com.netflix.iceberg.exceptions.RuntimeIOException;
import com.netflix.iceberg.expressions.Expression;
import com.netflix.iceberg.hadoop.HadoopInputFile;
import com.netflix.iceberg.io.InputFile;
import com.netflix.iceberg.parquet.Parquet;
import com.netflix.iceberg.spark.SparkFilters;
import com.netflix.iceberg.spark.SparkSchemaUtil;
import com.netflix.iceberg.spark.data.SparkAvroReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.execution.datasources.parquet.ParquetReadSupport;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.v2.reader.DataReader;
import org.apache.spark.sql.sources.v2.reader.DataSourceV2Reader;
import org.apache.spark.sql.sources.v2.reader.ReadTask;
import org.apache.spark.sql.sources.v2.reader.Statistics;
import org.apache.spark.sql.sources.v2.reader.SupportsPushDownFilters;
import org.apache.spark.sql.sources.v2.reader.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.sources.v2.reader.SupportsReportStatistics;
import org.apache.spark.sql.sources.v2.reader.SupportsScanUnsafeRow;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.SerializableConfiguration;
import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

import static com.netflix.iceberg.spark.SparkSchemaUtil.convert;
import static com.netflix.iceberg.spark.SparkSchemaUtil.prune;

class Reader implements DataSourceV2Reader, SupportsScanUnsafeRow,
    SupportsPushDownRequiredColumns, SupportsPushDownFilters, SupportsReportStatistics {
  private static final Filter[] NO_FILTERS = new Filter[0];
  private static final List<String> SNAPSHOT_COLUMNS = ImmutableList.of(
      "snapshot_id", "file_path", "file_ordinal", "file_format", "block_size_in_bytes",
      "file_size_in_bytes", "record_count", "partition"
  );

  private final Table table;
  private final String location;
  private final SerializableConfiguration conf;
  private Schema schema = null;
  private StructType type = null; // cached because Spark accesses it multiple times
  private List<Expression> filterExpressions = null;
  private Filter[] pushedFilters = NO_FILTERS;
  private List<FileScanTask> tasks = null; // lazy cache of tasks

  Reader(Table table, String location, Configuration conf) {
    this.table = table;
    this.location = location;
    this.conf = new SerializableConfiguration(conf);
    this.schema = table.schema();
    this.type = convert(schema);
  }

  @Override
  public StructType readSchema() {
    return type; // this is called from
  }

  @Override
  public List<ReadTask<UnsafeRow>> createUnsafeRowReadTasks() {
    String schemaString;
    try {
      schemaString = SchemaParser.toJson(schema);
    } catch (IOException e) {
      throw new RuntimeIOException(e);
    }

    List<ReadTask<UnsafeRow>> readTasks = Lists.newArrayList();
    for (FileScanTask fileTask : tasks()) {
      readTasks.add(new ScanTask(fileTask, schemaString, conf));
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

    return notSupported.toArray(new Filter[notSupported.size()]);
  }

  @Override
  public Filter[] pushedFilters() {
    return pushedFilters;
  }

  @Override
  public void pruneColumns(StructType requiredSchema) {
    this.schema = prune(table.schema(), requiredSchema);
    this.type = convert(schema);
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

      this.tasks = Lists.newArrayList(scan.planFiles());
    }

    return tasks;
  }

  @Override
  public String toString() {
    return String.format(
        "IcebergTable(location=%s, type=%s, filters=%s)",
        location, schema.asStruct(), filterExpressions);
  }

  private static class ScanTask implements ReadTask<UnsafeRow>, Serializable {
    // for some reason, the apply method can't be called from Java without reflection
    private static final DynMethods.UnboundMethod APPLY_PROJECTION = DynMethods.builder("apply")
        .impl(UnsafeProjection.class, InternalRow.class)
        .build();

    private final FileScanTask task;
    private final String schemaString;
    private final SerializableConfiguration conf;

    private transient Schema schema = null;
    private transient StructType type = null;

    private ScanTask(FileScanTask task, String schemaString, SerializableConfiguration conf) {
      this.task = task;
      this.schemaString = schemaString;
      this.conf = conf;
    }

    @Override
    public DataReader<UnsafeRow> createDataReader() {
      DataFile file = task.file();
      InputFile location = HadoopInputFile.fromLocation(file.path(), conf.value());
      Schema schema = lazySchema();

      switch (file.format()) {
        case PARQUET:
          // TODO: need to add the partition columns to the output rows for identity partitions
          Iterable<UnsafeRow> reader = Parquet.read(location)
              .project(schema)
              .split(task.start(), task.length())
              .readSupport(new ParquetReadSupport())
              .filter(task.residual())
              .set("org.apache.spark.sql.parquet.row.requested_schema", lazyType().json())
              .set("spark.sql.parquet.binaryAsString", "false")
              .set("spark.sql.parquet.int96AsTimestamp", "false")
              .callInit()
              .build();

          return new IteratorReader(reader.iterator());

        case AVRO:
          Iterable<InternalRow> avro = Avro.read(location)
              .reuseContainers()
              .project(schema)
              .split(task.start(), task.length())
              .createReaderFunc(SparkAvroReader::new)
              .build();

          return new IteratorReader(Iterators.transform(avro.iterator(),
              APPLY_PROJECTION.bind(UnsafeProjection.create(lazyType()))::invoke));

        default:
          throw new UnsupportedOperationException("Cannot read unknown format: " + file.format());
      }
    }

    private StructType lazyType() {
      if (type == null) {
        this.type = SparkSchemaUtil.convert(lazySchema());
      }
      return type;
    }

    private Schema lazySchema() {
      if (schema == null) {
        this.schema = SchemaParser.fromJson(schemaString);
      }
      return schema;
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
