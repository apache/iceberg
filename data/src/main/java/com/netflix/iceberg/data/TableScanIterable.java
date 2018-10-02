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

package com.netflix.iceberg.data;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.netflix.iceberg.CombinedScanTask;
import com.netflix.iceberg.FileScanTask;
import com.netflix.iceberg.HasTableOperations;
import com.netflix.iceberg.Schema;
import com.netflix.iceberg.TableOperations;
import com.netflix.iceberg.TableScan;
import com.netflix.iceberg.avro.Avro;
import com.netflix.iceberg.data.avro.DataReader;
import com.netflix.iceberg.exceptions.RuntimeIOException;
import com.netflix.iceberg.expressions.Binder;
import com.netflix.iceberg.expressions.Evaluator;
import com.netflix.iceberg.expressions.Expressions;
import com.netflix.iceberg.io.CloseableGroup;
import com.netflix.iceberg.io.CloseableIterable;
import com.netflix.iceberg.io.InputFile;
import com.netflix.iceberg.parquet.Parquet;
import com.netflix.iceberg.types.TypeUtil;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;
import static com.netflix.iceberg.data.parquet.GenericParquetReaders.buildReader;
import static java.util.Collections.emptyIterator;

class TableScanIterable extends CloseableGroup implements CloseableIterable<Record> {
  private static final List<String> SNAPSHOT_COLUMNS = ImmutableList.of(
      "snapshot_id", "file_path", "file_ordinal", "file_format", "block_size_in_bytes",
      "file_size_in_bytes", "record_count", "partition", "value_counts", "null_value_counts",
      "lower_bounds", "upper_bounds"
  );

  private final TableOperations ops;
  private final Schema projection;
  private final boolean reuseContainers;
  private final Iterable<CombinedScanTask> tasks;

  TableScanIterable(TableScan scan, List<String> columns, boolean reuseContainers) {
    Preconditions.checkArgument(scan.table() instanceof HasTableOperations,
        "Cannot scan table that doesn't expose its TableOperations");
    this.ops = ((HasTableOperations) scan.table()).operations();

    TableScan finalScan = scan.select(SNAPSHOT_COLUMNS);
    Set<Integer> requiredColumns = Binder.boundReferences(
        finalScan.table().schema().asStruct(), Collections.singletonList(scan.filter()));
    requiredColumns.addAll(TypeUtil.getProjectedIds(finalScan.table().schema().select(columns)));
    this.projection = TypeUtil.select(finalScan.table().schema(), requiredColumns);

    this.reuseContainers = reuseContainers;

    // start planning tasks in the background
    this.tasks = finalScan.planTasks();
  }

  @Override
  public Iterator<Record> iterator() {
    ScanIterator iter = new ScanIterator(tasks);
    addCloseable(iter);
    return iter;
  }

  private CloseableIterable<Record> open(FileScanTask task) {
    InputFile input = ops.newInputFile(task.file().path().toString());

    // TODO: join to partition data from the manifest file
    switch (task.file().format()) {
      case AVRO:
        Avro.ReadBuilder avro = Avro.read(input)
            .project(projection)
            .createReaderFunc(DataReader::create)
            .split(task.start(), task.length());

        if (reuseContainers) {
          avro.reuseContainers();
        }

        return avro.build();

      case PARQUET:
        Parquet.ReadBuilder parquet = Parquet.read(input)
            .project(projection)
            .createReaderFunc(fileSchema -> buildReader(projection, fileSchema))
            .split(task.start(), task.length());

        if (reuseContainers) {
          parquet.reuseContainers();
        }

        return parquet.build();

      default:
        throw new UnsupportedOperationException(String.format("Cannot read %s file: %s",
            task.file().format().name(), task.file().path()));
    }
  }

  private class ScanIterator implements Iterator<Record>, Closeable {
    private final Iterator<FileScanTask> tasks;
    private Closeable currentCloseable = null;
    private Iterator<Record> currentIterator = emptyIterator();

    private ScanIterator(Iterable<CombinedScanTask> tasks) {
      this.tasks = Lists.newArrayList(concat(transform(tasks, CombinedScanTask::files))).iterator();
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
            Evaluator filter = new Evaluator(projection.asStruct(), task.residual());
            this.currentIterator = filter(reader, filter::eval).iterator();
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
}
