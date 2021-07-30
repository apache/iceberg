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

public class MixedDeltaTaskWriter<T> extends BaseDeltaTaskWriter<T> {

  private final PartitionAwareWriter<T, DataWriteResult> dataWriter;
  private final PartitionAwareWriter<T, DeleteWriteResult> equalityDeleteWriter;
  private final PartitionAwareWriter<PositionDelete<T>, DeleteWriteResult> positionDeleteWriter;
  private final PositionDelete<T> positionDelete;

  public MixedDeltaTaskWriter(PartitionAwareWriter<T, DataWriteResult> dataWriter,
                              PartitionAwareWriter<T, DeleteWriteResult> equalityDeleteWriter,
                              PartitionAwareWriter<PositionDelete<T>, DeleteWriteResult> positionDeleteWriter,
                              FileIO io) {
    super(io);
    this.dataWriter = dataWriter;
    this.equalityDeleteWriter = equalityDeleteWriter;
    this.positionDeleteWriter = positionDeleteWriter;
    this.positionDelete = new PositionDelete<>();
  }

  @Override
  public void insert(T row, PartitionSpec spec, StructLike partition) throws IOException {
    dataWriter.write(row, spec, partition);
  }

  @Override
  public void delete(T row, PartitionSpec spec, StructLike partition) throws IOException {
    equalityDeleteWriter.write(row, spec, partition);
  }

  @Override
  public void delete(CharSequence path, long pos, T row, PartitionSpec spec, StructLike partition) throws IOException {
    positionDelete.set(path, pos, row);
    positionDeleteWriter.write(positionDelete, spec, partition);
  }

  @Override
  protected void closeWriters() throws IOException {
    if (dataWriter != null) {
      closeDataWriter(dataWriter);
    }

    if (equalityDeleteWriter != null) {
      closeDeleteWriter(equalityDeleteWriter);
    }

    if (positionDeleteWriter != null) {
      closeDeleteWriter(positionDeleteWriter);
    }
  }
}
