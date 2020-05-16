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

package org.apache.iceberg.data;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import org.apache.avro.generic.GenericData;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.avro.DataReader;
import org.apache.iceberg.data.orc.GenericOrcReader;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PartitionUtil;

class TableScanIterable extends CloseableGroup implements CloseableIterable<Record> {
  private final TableOperations ops;
  private final Schema projection;
  private final boolean reuseContainers;
  private final boolean caseSensitive;
  private final CloseableIterable<CombinedScanTask> tasks;

  TableScanIterable(TableScan scan, boolean reuseContainers) {
    Preconditions.checkArgument(scan.table() instanceof HasTableOperations,
        "Cannot scan table that doesn't expose its TableOperations");
    this.ops = ((HasTableOperations) scan.table()).operations();
    this.projection = scan.schema();
    this.reuseContainers = reuseContainers;
    this.caseSensitive = scan.isCaseSensitive();

    // start planning tasks in the background
    this.tasks = scan.planTasks();
  }

  @Override
  public CloseableIterator<Record> iterator() {
    ScanIterator iter = new ScanIterator(tasks, caseSensitive);
    addCloseable(iter);
    return iter;
  }

  private CloseableIterable<Record> open(FileScanTask task) {
    InputFile input = ops.io().newInputFile(task.file().path().toString());
    Map<Integer, ?> partition = PartitionUtil.constantsMap(task, TableScanIterable::convertConstant);

    // TODO: join to partition data from the manifest file
    switch (task.file().format()) {
      case AVRO:
        Avro.ReadBuilder avro = Avro.read(input)
            .project(projection)
            .createReaderFunc(
                avroSchema -> DataReader.create(projection, avroSchema, partition))
            .split(task.start(), task.length());

        if (reuseContainers) {
          avro.reuseContainers();
        }

        return avro.build();

      case PARQUET:
        Parquet.ReadBuilder parquet = Parquet.read(input)
            .project(projection)
            .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(projection, fileSchema, partition))
            .split(task.start(), task.length());

        if (reuseContainers) {
          parquet.reuseContainers();
        }

        return parquet.build();

      case ORC:
        ORC.ReadBuilder orc = ORC.read(input)
                .project(projection)
                .createReaderFunc(fileSchema -> GenericOrcReader.buildReader(projection, fileSchema))
                .split(task.start(), task.length());

        return orc.build();

      default:
        throw new UnsupportedOperationException(String.format("Cannot read %s file: %s",
            task.file().format().name(), task.file().path()));
    }
  }

  @Override
  public void close() throws IOException {
    tasks.close(); // close manifests from scan planning
    super.close(); // close data files
  }

  private class ScanIterator implements CloseableIterator<Record> {
    private final Iterator<FileScanTask> tasks;
    private final boolean caseSensitive;
    private Closeable currentCloseable = null;
    private Iterator<Record> currentIterator = Collections.emptyIterator();

    private ScanIterator(CloseableIterable<CombinedScanTask> tasks, boolean caseSensitive) {
      this.tasks = Lists.newArrayList(Iterables.concat(
          CloseableIterable.transform(tasks, CombinedScanTask::files))).iterator();
      this.caseSensitive = caseSensitive;
    }

    @Override
    public boolean hasNext() {
      while (true) {
        if (currentIterator.hasNext()) {
          return true;

        } else if (tasks.hasNext()) {
          if (currentCloseable != null) {
            try {
              currentCloseable.close();
            } catch (IOException e) {
              throw new RuntimeIOException(e, "Failed to close task");
            }
          }

          FileScanTask task = tasks.next();
          CloseableIterable<Record> reader = open(task);
          this.currentCloseable = reader;

          if (task.residual() != null && task.residual() != Expressions.alwaysTrue()) {
            Evaluator filter = new Evaluator(projection.asStruct(), task.residual(), caseSensitive);
            this.currentIterator = Iterables.filter(reader, filter::eval).iterator();
          } else {
            this.currentIterator = reader.iterator();
          }

        } else {
          return false;
        }
      }
    }

    @Override
    public Record next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }

      return currentIterator.next();
    }

    @Override
    public void close() throws IOException {
      if (currentCloseable != null) {
        currentCloseable.close();
      }
    }
  }

  /**
   * Conversions from generic Avro values to Iceberg generic values.
   */
  private static Object convertConstant(Type type, Object value) {
    if (value == null) {
      return null;
    }

    switch (type.typeId()) {
      case STRING:
        return value.toString();
      case TIME:
        return DateTimeUtil.timeFromMicros((Long) value);
      case DATE:
        return DateTimeUtil.dateFromDays((Integer) value);
      case TIMESTAMP:
        if (((Types.TimestampType) type).shouldAdjustToUTC()) {
          return DateTimeUtil.timestamptzFromMicros((Long) value);
        } else {
          return DateTimeUtil.timestampFromMicros((Long) value);
        }
      case FIXED:
        if (value instanceof GenericData.Fixed) {
          return ((GenericData.Fixed) value).bytes();
        }
        return value;
      default:
    }
    return value;
  }
}
