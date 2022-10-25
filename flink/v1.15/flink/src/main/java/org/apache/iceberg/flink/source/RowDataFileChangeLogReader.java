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
package org.apache.iceberg.flink.source;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.apache.iceberg.AddedRowsScanTask;
import org.apache.iceberg.ChangelogScanTask;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.DeletedDataFileScanTask;
import org.apache.iceberg.DeletedRowsScanTask;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.Schema;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.flink.data.RowDataProjection;
import org.apache.iceberg.flink.data.RowDataUtil;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.PartitionUtil;

@Internal
public class RowDataFileChangeLogReader extends RowDataFileScanTaskReader {

  public RowDataFileChangeLogReader(
      Schema tableSchema,
      Schema projectedSchema,
      boolean caseSensitive,
      String nameMapping,
      ScanTaskGroup<? extends ScanTask> taskGroup,
      FileIO io,
      EncryptionManager encryption,
      List<Expression> filters) {
    super(
        tableSchema,
        projectedSchema,
        nameMapping,
        caseSensitive,
        taskGroup,
        io,
        encryption,
        filters);
  }

  @Override
  protected Stream<ContentFile<?>> referencedFiles(ScanTask task) {
    if (task instanceof AddedRowsScanTask) {
      return addedRowsScanTaskFiles((AddedRowsScanTask) task);
    } else if (task instanceof DeletedRowsScanTask) {
      throw new UnsupportedOperationException("Deleted rows scan task is not supported yet");
    } else if (task instanceof DeletedDataFileScanTask) {
      return deletedDataFileScanTaskFiles((DeletedDataFileScanTask) task);
    } else {
      throw new IllegalArgumentException(
          "Unsupported changelog scan task type: " + task.getClass().getName());
    }
  }

  @Override
  public CloseableIterator<RowData> open(ScanTask scanTask) {
    return openChangelogScanTask((ChangelogScanTask) scanTask, projectedSchema()).iterator();
  }

  private CloseableIterable<RowData> openChangelogScanTask(
      ChangelogScanTask task, Schema requestedSchema) {
    if (task instanceof AddedRowsScanTask) {
      return openAddedRowsScanTask((AddedRowsScanTask) task, requestedSchema);
    } else if (task instanceof DeletedRowsScanTask) {
      throw new UnsupportedOperationException("Deleted rows scan task is not supported yet");
    } else if (task instanceof DeletedDataFileScanTask) {
      return openDeletedDataFileScanTask((DeletedDataFileScanTask) task, requestedSchema);
    } else {
      throw new IllegalArgumentException(
          "Unsupported changelog scan task type: " + task.getClass().getName());
    }
  }

  private CloseableIterable<RowData> openAddedRowsScanTask(
      AddedRowsScanTask task, Schema requestedSchema) {
    DataFile file = task.file();
    String filePath = file.path().toString();
    FileFormat format = file.format();
    long start = task.start();
    long length = task.length();
    Expression residual = task.residual();
    InputFile inputFile = inputFile(filePath);

    FlinkDeleteFilter deletes =
        new FlinkDeleteFilter(filePath, task.deletes(), tableSchema(), requestedSchema);

    Schema partitionSchema = TypeUtil.select(projectedSchema(), task.spec().identitySourceIds());

    Map<Integer, ?> idToConstant =
        partitionSchema.columns().isEmpty()
            ? ImmutableMap.of()
            : PartitionUtil.constantsMap(task, RowDataUtil::convertConstant);

    return getRowData(
        format, start, length, residual, inputFile, deletes, idToConstant, RowKind.INSERT);
  }

  private CloseableIterable<RowData> openDeletedDataFileScanTask(
      DeletedDataFileScanTask task, Schema requestedSchema) {
    DataFile file = task.file();
    long start = task.start();
    long length = task.length();
    Expression residual = task.residual();
    String filePath = file.path().toString();
    FileFormat format = file.format();
    InputFile inputFile = inputFile(filePath);

    FlinkDeleteFilter deletes =
        new FlinkDeleteFilter(filePath, task.existingDeletes(), tableSchema(), requestedSchema);
    Schema partitionSchema = TypeUtil.select(projectedSchema(), task.spec().identitySourceIds());

    Map<Integer, ?> idToConstant =
        partitionSchema.columns().isEmpty()
            ? ImmutableMap.of()
            : PartitionUtil.constantsMap(task, RowDataUtil::convertConstant);

    return getRowData(
        format, start, length, residual, inputFile, deletes, idToConstant, RowKind.DELETE);
  }

  private static Stream<ContentFile<?>> deletedDataFileScanTaskFiles(DeletedDataFileScanTask task) {
    DeletedDataFileScanTask deletedDataFileScanTask = task;
    DataFile file = deletedDataFileScanTask.file();
    List<DeleteFile> existingDeletes = deletedDataFileScanTask.existingDeletes();
    return Stream.concat(Stream.of(file), existingDeletes.stream());
  }

  private CloseableIterable<RowData> getRowData(
      FileFormat format,
      long start,
      long length,
      Expression residual,
      InputFile inputFile,
      FlinkDeleteFilter deletes,
      Map<Integer, ?> idToConstant,
      RowKind rowKind) {
    CloseableIterable<RowData> iterable =
        deletes.filter(
            newIterable(
                inputFile,
                format,
                start,
                length,
                residual,
                deletes.requiredSchema(),
                idToConstant));

    CloseableIterable<RowData> dataWithKind =
        CloseableIterable.transform(
            iterable,
            rowData -> {
              rowData.setRowKind(rowKind);
              return rowData;
            });

    // Project the RowData to remove the extra meta columns.
    if (!projectedSchema().sameSchema(deletes.requiredSchema())) {
      RowDataProjection rowDataProjection =
          RowDataProjection.create(
              deletes.requiredRowType(),
              deletes.requiredSchema().asStruct(),
              projectedSchema().asStruct());
      return CloseableIterable.transform(dataWithKind, rowDataProjection::wrap);
    }

    return dataWithKind;
  }

  private static Stream<ContentFile<?>> addedRowsScanTaskFiles(AddedRowsScanTask task) {
    AddedRowsScanTask addedRowsScanTask = task;
    DataFile file = addedRowsScanTask.file();
    List<DeleteFile> deletes = addedRowsScanTask.deletes();
    return Stream.concat(Stream.of(file), deletes.stream());
  }
}
