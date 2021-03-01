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

package org.apache.iceberg.data.arrow;

import java.io.IOException;
import java.util.Iterator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.types.Types;

/**
 * Vectorized reader that returns an iterator of {@link VectorSchemaRoot}. See {@link #iterator()} to learn about the
 * behavior of the iterator.
 *
 * <p>The following Iceberg data types are supported and have been tested:
 * <ul>
 *     <li>Iceberg: {@link Types.BooleanType}, Arrow: {@link MinorType#BIT}</li>
 *     <li>Iceberg: {@link Types.IntegerType}, Arrow: {@link MinorType#INT}</li>
 *     <li>Iceberg: {@link Types.LongType}, Arrow: {@link MinorType#BIGINT}</li>
 *     <li>Iceberg: {@link Types.FloatType}, Arrow: {@link MinorType#FLOAT4}</li>
 *     <li>Iceberg: {@link Types.DoubleType}, Arrow: {@link MinorType#FLOAT8}</li>
 *     <li>Iceberg: {@link Types.StringType}, Arrow: {@link MinorType#VARCHAR}</li>
 *     <li>Iceberg: {@link Types.TimestampType} (both with and without timezone),
 *         Arrow: {@link MinorType#TIMEMICRO}</li>
 *     <li>Iceberg: {@link Types.BinaryType}, Arrow: {@link MinorType#VARBINARY}</li>
 *     <li>Iceberg: {@link Types.DateType}, Arrow: {@link MinorType#DATEDAY}</li>
 * </ul>
 *
 * <p>Features that don't work in this implementation:
 * <ul>
 *     <li>Type promotion: In case of type promotion, the Arrow vector corresponding to
 *     the data type in the parquet file is returned instead of the data type in the latest schema.</li>
 *     <li>Columns with constant values are physically encoded as a dictionary. The Arrow vector
 *     type is int32 instead of the type as per the schema.</li>
 *     <li>Data types: {@link Types.TimeType}, {@link Types.ListType}, {@link Types.MapType},
 *     {@link Types.StructType}</li>
 * </ul>
 *
 * <p>Data types not tested: {@link Types.UUIDType}, {@link Types.FixedType}, {@link Types.DecimalType}.
 */
public class ArrowReader extends CloseableGroup implements CloseableIterable<VectorSchemaRoot> {

  private static final int BATCH_SIZE_IN_NUM_ROWS = 1 << 16;

  /**
   * Builder for the {@link ArrowReader}.
   */
  public static final class Builder {
    private final TableScan scan;
    private int batchSize;
    private boolean reuseContainers;

    private Builder(TableScan scan) {
      this.scan = scan;
      this.batchSize = BATCH_SIZE_IN_NUM_ROWS;
    }

    /**
     * Set the maximum number of rows per Arrow batch.. The default value is {@link #BATCH_SIZE_IN_NUM_ROWS}.
     */
    public Builder setBatchSize(int batchSize) {
      this.batchSize = batchSize;
      return this;
    }

    /**
     * Set whether to reuse Arrow vectors. The default value is {@code false}.
     *
     * <p>If {@code false}, every {@link Iterator#next()} call returns a new
     * instance of {@link VectorSchemaRoot} and the containing Arrow vectors. The Arrow vectors in the previous {@link
     * VectorSchemaRoot} are closed before returning the next {@link VectorSchemaRoot} object.
     *
     * <p>If {@code true}, the Arrow vectors in the previous
     * {@link VectorSchemaRoot} may be reused for the next {@link VectorSchemaRoot}.
     */
    public Builder setReuseContainers(boolean reuseContainers) {
      this.reuseContainers = reuseContainers;
      return this;
    }

    /**
     * Build the {@link ArrowReader}.
     */
    public ArrowReader build() {
      return new ArrowReader(scan, batchSize, reuseContainers);
    }
  }

  /**
   * Create a new builder.
   */
  public static Builder newBuilder(TableScan scan) {
    return new Builder(scan);
  }

  private final CloseableIterable<CombinedScanTask> tasks;
  private final Schema schema;
  private final FileIO io;
  private final EncryptionManager encryption;
  private final int batchSize;
  private final boolean reuseContainers;

  private ArrowReader(TableScan scan, int batchSize, boolean reuseContainers) {
    this.schema = scan.schema();
    this.io = scan.table().io();
    this.encryption = scan.table().encryption();
    this.batchSize = batchSize;
    // start planning tasks in the background
    this.tasks = scan.planTasks();
    this.reuseContainers = reuseContainers;
  }

  /**
   * Returns a new iterator of {@link VectorSchemaRoot} objects.
   * <p>
   * Note that the reader owns the {@link VectorSchemaRoot} objects and takes care of closing them.
   * The caller should not hold onto a {@link VectorSchemaRoot} or try to close them.
   *
   * <p>If {@code reuseContainers} is {@code false}, the Arrow vectors in the
   * previous {@link VectorSchemaRoot} are closed before returning the next {@link VectorSchemaRoot} object.
   * This implies that the caller should either use the {@link VectorSchemaRoot} or transfer the ownership of
   * {@link VectorSchemaRoot} before getting the next {@link VectorSchemaRoot}.
   *
   * <p>If {@code reuseContainers} is {@code true}, the Arrow vectors in the
   * previous {@link VectorSchemaRoot} may be reused for the next {@link VectorSchemaRoot}.
   * This implies that the caller should either use the {@link VectorSchemaRoot} or deep copy the
   * {@link VectorSchemaRoot} before getting the next {@link VectorSchemaRoot}.
   */
  @Override
  public CloseableIterator<VectorSchemaRoot> iterator() {
    CloseableIterator<VectorSchemaRoot> iter = open();
    addCloseable(iter);
    return iter;
  }

  private CloseableIterator<VectorSchemaRoot> open() {
    return new ConcatIterator();
  }

  private CloseableIterator<VectorSchemaRoot> open(CombinedScanTask task) {
    return new ArrowFileScanTaskReader(
        task,
        schema,
        null,
        io,
        encryption,
        true,
        batchSize,
        reuseContainers
    );
  }

  @Override
  public void close() throws IOException {
    tasks.close(); // close manifests from scan planning
    super.close(); // close data files
  }

  private final class ConcatIterator implements CloseableIterator<VectorSchemaRoot> {
    private CloseableIterator<VectorSchemaRoot> currentIterator = CloseableIterator.empty();
    private final CloseableIterator<CombinedScanTask> tasksIterator = tasks.iterator();
    private VectorSchemaRoot current;

    @Override
    public void close() throws IOException {
      this.currentIterator.close();
    }

    @Override
    public boolean hasNext() {
      try {
        while (true) {
          if (currentIterator.hasNext()) {
            this.current = currentIterator.next();
            return true;
          } else if (tasksIterator.hasNext()) {
            this.currentIterator.close();
            this.currentIterator = open(tasksIterator.next());
          } else {
            this.currentIterator.close();
            return false;
          }
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public VectorSchemaRoot next() {
      return current;
    }
  }
}
