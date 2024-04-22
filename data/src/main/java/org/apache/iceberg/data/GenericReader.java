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

import java.io.Serializable;
import java.util.Map;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.avro.DataReader;
import org.apache.iceberg.data.orc.GenericOrcReader;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.encryption.EncryptingFileIO;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.PartitionUtil;

class GenericReader implements Serializable {
  private final EncryptingFileIO io;
  private final Schema tableSchema;
  private final Schema projection;
  private final boolean caseSensitive;
  private final boolean reuseContainers;

  GenericReader(TableScan scan, boolean reuseContainers) {
    this.io = EncryptingFileIO.combine(scan.table().io(), scan.table().encryption());
    this.tableSchema = scan.table().schema();
    this.projection = scan.schema();
    this.caseSensitive = scan.isCaseSensitive();
    this.reuseContainers = reuseContainers;
  }

  CloseableIterator<Record> open(CloseableIterable<CombinedScanTask> tasks) {
    Iterable<FileScanTask> fileTasks =
        Iterables.concat(Iterables.transform(tasks, CombinedScanTask::files));
    return CloseableIterable.concat(Iterables.transform(fileTasks, this::open)).iterator();
  }

  public CloseableIterable<Record> open(CombinedScanTask task) {
    return new CombinedTaskIterable(task);
  }

  public CloseableIterable<Record> open(FileScanTask task) {
    DeleteFilter<Record> deletes = new GenericDeleteFilter(io, task, tableSchema, projection);
    Schema readSchema = deletes.requiredSchema();

    CloseableIterable<Record> records = openFile(task, readSchema);
    records = deletes.filter(records);
    records = applyResidual(records, readSchema, task.residual());

    return records;
  }

  private CloseableIterable<Record> applyResidual(
      CloseableIterable<Record> records, Schema recordSchema, Expression residual) {
    if (residual != null && residual != Expressions.alwaysTrue()) {
      InternalRecordWrapper wrapper = new InternalRecordWrapper(recordSchema.asStruct());
      Evaluator filter = new Evaluator(recordSchema.asStruct(), residual, caseSensitive);
      return CloseableIterable.filter(records, record -> filter.eval(wrapper.wrap(record)));
    }

    return records;
  }

  private CloseableIterable<Record> openFile(FileScanTask task, Schema fileProjection) {
    InputFile input = io.newInputFile(task.file());
    Map<Integer, ?> partition =
        PartitionUtil.constantsMap(task, IdentityPartitionConverters::convertConstant);

    switch (task.file().format()) {
      case AVRO:
        Avro.ReadBuilder avro =
            Avro.read(input)
                .project(fileProjection)
                .createReaderFunc(
                    avroSchema -> DataReader.create(fileProjection, avroSchema, partition))
                .split(task.start(), task.length());

        if (reuseContainers) {
          avro.reuseContainers();
        }

        return avro.build();

      case PARQUET:
        Parquet.ReadBuilder parquet =
            Parquet.read(input)
                .project(fileProjection)
                .createReaderFunc(
                    fileSchema ->
                        GenericParquetReaders.buildReader(fileProjection, fileSchema, partition))
                .split(task.start(), task.length())
                .caseSensitive(caseSensitive)
                .filter(task.residual());

        if (reuseContainers) {
          parquet.reuseContainers();
        }

        return parquet.build();

      case ORC:
        Schema projectionWithoutConstantAndMetadataFields =
            TypeUtil.selectNot(
                fileProjection, Sets.union(partition.keySet(), MetadataColumns.metadataFieldIds()));
        ORC.ReadBuilder orc =
            ORC.read(input)
                .project(projectionWithoutConstantAndMetadataFields)
                .createReaderFunc(
                    fileSchema ->
                        GenericOrcReader.buildReader(fileProjection, fileSchema, partition))
                .split(task.start(), task.length())
                .caseSensitive(caseSensitive)
                .filter(task.residual());

        return orc.build();

      default:
        throw new UnsupportedOperationException(
            String.format(
                "Cannot read %s file: %s", task.file().format().name(), task.file().path()));
    }
  }

  private class CombinedTaskIterable extends CloseableGroup implements CloseableIterable<Record> {
    private final CombinedScanTask task;

    private CombinedTaskIterable(CombinedScanTask task) {
      this.task = task;
    }

    @Override
    public CloseableIterator<Record> iterator() {
      CloseableIterator<Record> iter =
          CloseableIterable.concat(Iterables.transform(task.files(), GenericReader.this::open))
              .iterator();
      addCloseable(iter);
      return iter;
    }
  }
}
