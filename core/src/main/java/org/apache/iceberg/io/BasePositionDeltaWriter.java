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
package org.apache.iceberg.io;

import java.io.IOException;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public class BasePositionDeltaWriter<T> implements PositionDeltaWriter<T> {

  private final PartitioningWriter<T, DataWriteResult> insertWriter;
  private final PartitioningWriter<T, DataWriteResult> updateWriter;
  private final PartitioningWriter<PositionDelete<T>, DeleteWriteResult> deleteWriter;
  private final PositionDelete<T> positionDelete;

  private boolean closed;

  public BasePositionDeltaWriter(
      PartitioningWriter<T, DataWriteResult> insertWriter,
      PartitioningWriter<T, DataWriteResult> updateWriter,
      PartitioningWriter<PositionDelete<T>, DeleteWriteResult> deleteWriter) {
    Preconditions.checkArgument(insertWriter != null, "Insert writer cannot be null");
    Preconditions.checkArgument(updateWriter != null, "Update writer cannot be null");
    Preconditions.checkArgument(
        insertWriter != updateWriter, "Update and insert writers must be different");
    Preconditions.checkArgument(deleteWriter != null, "Delete writer cannot be null");

    this.insertWriter = insertWriter;
    this.updateWriter = updateWriter;
    this.deleteWriter = deleteWriter;
    this.positionDelete = PositionDelete.create();
  }

  @Override
  public void insert(T row, PartitionSpec spec, StructLike partition) {
    insertWriter.write(row, spec, partition);
  }

  @Override
  public void update(T row, PartitionSpec spec, StructLike partition) {
    updateWriter.write(row, spec, partition);
  }

  @Override
  public void delete(CharSequence path, long pos, T row, PartitionSpec spec, StructLike partition) {
    positionDelete.set(path, pos, row);
    deleteWriter.write(positionDelete, spec, partition);
  }

  @Override
  public WriteResult result() {
    Preconditions.checkState(closed, "Cannot get result from unclosed writer");

    DataWriteResult insertWriteResult = insertWriter.result();
    DataWriteResult updateWriteResult = updateWriter.result();
    DeleteWriteResult deleteWriteResult = deleteWriter.result();

    return WriteResult.builder()
        .addDataFiles(insertWriteResult.dataFiles())
        .addDataFiles(updateWriteResult.dataFiles())
        .addDeleteFiles(deleteWriteResult.deleteFiles())
        .addReferencedDataFiles(deleteWriteResult.referencedDataFiles())
        .build();
  }

  @Override
  public void close() throws IOException {
    if (!closed) {
      insertWriter.close();
      updateWriter.close();
      deleteWriter.close();

      this.closed = true;
    }
  }
}
