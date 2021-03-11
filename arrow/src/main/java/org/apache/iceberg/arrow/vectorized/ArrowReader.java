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

package org.apache.iceberg.arrow.vectorized;

import java.io.IOException;
import java.util.Iterator;
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
 * Vectorized reader that returns an iterator of {@link ArrowBatch}.
 * See {@link #open(CloseableIterable)} ()} to learn about the
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
public class ArrowReader extends CloseableGroup {

  private final Schema schema;
  private final FileIO io;
  private final EncryptionManager encryption;
  private final int batchSize;
  private final boolean reuseContainers;

  /**
   * Create a new instance of the reader.
   *
   * @param scan the table scan object.
   * @param batchSize the maximum number of rows per Arrow batch.
   * @param reuseContainers whether to reuse Arrow vectors when iterating through the data.
   *                        If set to {@code false}, every {@link Iterator#next()} call creates
   *                        new instances of Arrow vectors.
   *                        If set to {@code true}, the Arrow vectors in the previous
   *                        {@link Iterator#next()} may be reused for the data returned
   *                        in the current {@link Iterator#next()}.
   *                        This option avoids allocating memory again and again.
   *                        Irrespective of the value of {@code reuseContainers}, the Arrow vectors
   *                        in the previous {@link Iterator#next()} call are closed before creating
   *                        new instances if the current {@link Iterator#next()}.
   */
  public ArrowReader(TableScan scan, int batchSize, boolean reuseContainers) {
    this.schema = scan.schema();
    this.io = scan.table().io();
    this.encryption = scan.table().encryption();
    this.batchSize = batchSize;
    // start planning tasks in the background
    this.reuseContainers = reuseContainers;
  }

  /**
   * Returns a new iterator of {@link ArrowBatch} objects.
   * <p>
   * Note that the reader owns the {@link ArrowBatch} objects and takes care of closing them.
   * The caller should not hold onto a {@link ArrowBatch} or try to close them.
   *
   * <p>If {@code reuseContainers} is {@code false}, the Arrow vectors in the
   * previous {@link ArrowBatch} are closed before returning the next {@link ArrowBatch} object.
   * This implies that the caller should either use the {@link ArrowBatch} or transfer the ownership of
   * {@link ArrowBatch} before getting the next {@link ArrowBatch}.
   *
   * <p>If {@code reuseContainers} is {@code true}, the Arrow vectors in the
   * previous {@link ArrowBatch} may be reused for the next {@link ArrowBatch}.
   * This implies that the caller should either use the {@link ArrowBatch} or deep copy the
   * {@link ArrowBatch} before getting the next {@link ArrowBatch}.
   */
  public CloseableIterator<ArrowBatch> open(CloseableIterable<CombinedScanTask> tasks) {
    CloseableIterator<ArrowBatch> itr = new ConcatIterator(tasks);
    addCloseable(itr);
    return itr;
  }

  private CloseableIterator<ArrowBatch> open(CombinedScanTask task) {
    return new VectorizedCombinedScanIterator(
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
    super.close(); // close data files
  }

  private final class ConcatIterator implements CloseableIterator<ArrowBatch> {
    private CloseableIterator<ArrowBatch> currentIterator;
    private final CloseableIterator<CombinedScanTask> tasksIterator;
    private ArrowBatch current;

    private ConcatIterator(CloseableIterable<CombinedScanTask> tasks) {
      this.currentIterator = CloseableIterator.empty();
      this.tasksIterator = tasks.iterator();
    }

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
    public ArrowBatch next() {
      return current;
    }
  }
}
