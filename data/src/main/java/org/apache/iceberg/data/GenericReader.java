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
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.encryption.EncryptingFileIO;
import org.apache.iceberg.encryption.InputFilesDecryptor;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.formats.DataFileReadBuilder;
import org.apache.iceberg.formats.ReadBuilder;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
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
    return CloseableIterable.concat(Iterables.transform(tasks, this::open)).iterator();
  }

  public CloseableIterable<Record> open(CombinedScanTask task) {
    return new CombinedTaskIterable(task);
  }

  private CloseableIterable<Record> open(FileScanTask task, InputFilesDecryptor decryptor) {
    DeleteFilter<Record> deletes =
        new GenericDeleteFilter(decryptor::getInputFile, task, tableSchema, projection);
    Schema readSchema = deletes.requiredSchema();

    CloseableIterable<Record> records = openFile(task, readSchema, decryptor);
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

  private CloseableIterable<Record> openFile(
      FileScanTask task, Schema fileProjection, InputFilesDecryptor decryptor) {
    Map<Integer, ?> partition =
        PartitionUtil.constantsMap(task, IdentityPartitionConverters::convertConstant);

    ReadBuilder<Record, ?> builder =
        DataFileReadBuilder.read(task.file(), Record.class, decryptor::getInputFile);
    if (reuseContainers) {
      builder = builder.reuseContainers();
    }

    return builder
        .project(fileProjection)
        .idToConstant(partition)
        .split(task.start(), task.length())
        .caseSensitive(caseSensitive)
        .filter(task.residual())
        .build();
  }

  private class CombinedTaskIterable extends CloseableGroup implements CloseableIterable<Record> {
    private final CombinedScanTask task;

    private CombinedTaskIterable(CombinedScanTask task) {
      this.task = task;
    }

    @Override
    public CloseableIterator<Record> iterator() {
      InputFilesDecryptor decryptor = InputFilesDecryptor.fromTasks(task.files(), io);
      CloseableIterator<Record> iter =
          CloseableIterable.concat(
                  Iterables.transform(task.files(), fileTask -> open(fileTask, decryptor)))
              .iterator();
      addCloseable(iter);
      return iter;
    }
  }
}
