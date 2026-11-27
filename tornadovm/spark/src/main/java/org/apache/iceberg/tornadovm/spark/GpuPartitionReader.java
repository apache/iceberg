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
package org.apache.iceberg.tornadovm.spark;

import java.util.Iterator;
import java.util.List;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.tornadovm.DeviceMergeOnReadScan;
import org.apache.iceberg.tornadovm.LiveRows;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

/**
 * A Spark task's data files, read on the GPU: the device scan decodes them and applies their
 * deletes, and each batch of live rows it returns is copied into Spark column vectors, in the order
 * Spark expects. The task's batches are read on the first call to {@link #next()}.
 */
final class GpuPartitionReader implements PartitionReader<ColumnarBatch> {
  /** Spark tasks read on the device in this JVM, for tests and logs. */
  static final java.util.concurrent.atomic.AtomicLong TASKS =
      new java.util.concurrent.atomic.AtomicLong();

  /** Spark tasks the device refused and Iceberg's reader read instead, for tests and logs. */
  static final java.util.concurrent.atomic.AtomicLong REFUSED =
      new java.util.concurrent.atomic.AtomicLong();

  private static final boolean DEBUG = Boolean.getBoolean("iceberg.tornadovm.spark.debug");
  private static final org.slf4j.Logger LOG =
      org.slf4j.LoggerFactory.getLogger(GpuPartitionReader.class);

  private final DeviceMergeOnReadScan scan;
  private final List<FileScanTask> tasks;
  private final Schema schema;
  private final StructType sparkSchema;
  private final java.util.function.Supplier<PartitionReader<ColumnarBatch>> cpu;
  private Iterator<ColumnarBatch> batches;
  private ColumnarBatch current;
  // Iceberg's reader, once the device refused the group.
  private PartitionReader<ColumnarBatch> fallback;

  GpuPartitionReader(
      DeviceMergeOnReadScan scan,
      ScanTaskGroup<FileScanTask> taskGroup,
      Schema schema,
      java.util.function.Supplier<PartitionReader<ColumnarBatch>> cpu) {
    this.scan = scan;
    this.tasks = Lists.newArrayList(taskGroup.tasks());
    this.schema = schema;
    this.sparkSchema = SparkSchemaUtil.convert(schema);
    this.cpu = cpu;
  }

  /**
   * The group's batches, from the device; when the device refuses the group as it plans it (a file
   * whose string column is beyond its share of the one string buffer, ClickBench q24 on hits with
   * equality deletes), before any batch, Iceberg's reader reads it instead of the query failing.
   */
  @Override
  public boolean next() throws java.io.IOException {
    if (fallback != null) {
      return fallback.next();
    }
    if (batches == null) {
      // The scan hands batches over from each of its stream threads at once.
      List<ColumnarBatch> read = java.util.Collections.synchronizedList(Lists.newArrayList());
      long start = System.nanoTime();
      List<String> columns = Lists.newArrayList();
      for (Types.NestedField field : schema.columns()) {
        columns.add(field.name());
      }
      java.util.concurrent.atomic.AtomicLong convert = new java.util.concurrent.atomic.AtomicLong();
      long rows;
      try {
        rows =
            scan.materialize(
                tasks,
                columns,
                batch -> {
                  long converting = System.nanoTime();
                  read.add(toSpark(batch));
                  convert.addAndGet(System.nanoTime() - converting);
                });
      } catch (UnsupportedOperationException e) {
        if (cpu == null || !read.isEmpty()) {
          throw e;
        }
        REFUSED.incrementAndGet();
        LOG.info("Reading {} files on the CPU: {}", tasks.size(), e.getMessage());
        LOG.debug("Device refusal", e);
        fallback = cpu.get();
        return fallback.next();
      }
      TASKS.incrementAndGet();
      if (DEBUG) {
        LOG.info(
            "[gpu-spark] t={}..{} {} files, {} live rows, {} batches in {} ms (to Spark {}) | {}",
            java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(start) % 100_000,
            java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(System.nanoTime()) % 100_000,
            tasks.size(),
            rows,
            read.size(),
            String.format(java.util.Locale.ROOT, "%.1f", (System.nanoTime() - start) / 1e6),
            String.format(java.util.Locale.ROOT, "%.1f", convert.get() / 1e6),
            scan.metrics());
      }
      batches = read.iterator();
    }
    if (current != null) {
      current.close();
      current = null;
    }
    if (batches.hasNext()) {
      current = batches.next();
      return true;
    }
    return false;
  }

  @Override
  public ColumnarBatch get() {
    return fallback != null ? fallback.get() : current;
  }

  @Override
  public void close() throws java.io.IOException {
    if (fallback != null) {
      fallback.close();
    }
    if (current != null) {
      current.close();
    }
    while (batches != null && batches.hasNext()) {
      batches.next().close();
    }
  }

  /**
   * One device batch as Spark columns: a bulk copy a column into heap arrays, which the device
   * batch's buffers -- reused for the next batch -- are not.
   */
  private ColumnarBatch toSpark(LiveRows batch) {
    int rows = batch.rows();
    List<Types.NestedField> fields = schema.columns();
    ColumnVector[] vectors = new ColumnVector[fields.size()];
    for (int c = 0; c < fields.size(); c++) {
      Types.NestedField field = fields.get(c);
      org.apache.spark.sql.types.DataType type = sparkSchema.fields()[c].dataType();
      int column = batch.column(field.name());
      boolean[] nulls = new boolean[rows];
      if (!batch.copyNulls(column, nulls)) {
        nulls = null;
      }
      Type.TypeID id = field.type().typeId();
      vectors[c] =
          switch (id) {
            case INTEGER, DATE -> {
              int[] values = new int[rows];
              batch.copyInts(column, values);
              yield HostColumnVector.ofInts(type, values, nulls);
            }
            case LONG, TIMESTAMP, DECIMAL -> {
              long[] values = new long[rows];
              batch.copyLongs(column, values);
              yield HostColumnVector.ofLongs(type, values, nulls);
            }
            case FLOAT, DOUBLE -> {
              double[] values = new double[rows];
              batch.copyDoubles(column, values);
              yield HostColumnVector.ofDoubles(type, values, nulls);
            }
            case STRING -> HostColumnVector.ofStrings(type, batch.utf8Column(column), nulls);
            default -> throw new UnsupportedOperationException("Column " + field);
          };
    }
    return new ColumnarBatch(vectors, rows);
  }
}
