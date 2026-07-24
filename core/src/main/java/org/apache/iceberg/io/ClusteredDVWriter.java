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
import java.util.function.Function;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.deletes.ClusteredDVFileWriter;
import org.apache.iceberg.deletes.DVFileWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * A {@link PartitioningWriter} for deletion vectors that requires the incoming deletes to be
 * clustered by data file (e.g. sorted by spec, partition, file and position). Unlike {@link
 * PartitioningDVWriter}, which buffers one position index per touched data file until close, this
 * writer keeps a single live position index and flushes each data file's deletion vector as soon as
 * the input moves on to the next file, bounding the task's resident state to one data file.
 */
public class ClusteredDVWriter<T>
    implements PartitioningWriter<PositionDelete<T>, DeleteWriteResult> {

  private final DVFileWriter writer;
  private DeleteWriteResult result;

  public ClusteredDVWriter(
      OutputFileFactory fileFactory,
      Function<CharSequence, PositionDeleteIndex> loadPreviousDeletes) {
    this.writer = new ClusteredDVFileWriter(fileFactory, loadPreviousDeletes::apply);
  }

  @Override
  public void write(PositionDelete<T> row, PartitionSpec spec, StructLike partition) {
    writer.delete(row.path().toString(), row.pos(), spec, partition);
  }

  @Override
  public DeleteWriteResult result() {
    Preconditions.checkState(result != null, "Cannot get result from unclosed writer");
    return result;
  }

  @Override
  public void close() throws IOException {
    if (result == null) {
      writer.close();
      this.result = writer.result();
    }
  }
}
