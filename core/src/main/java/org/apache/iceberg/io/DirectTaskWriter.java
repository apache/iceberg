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
import org.apache.iceberg.util.Tasks;

public class DirectTaskWriter<T> implements TaskWriter<T> {
  @SuppressWarnings("rawtypes")
  private static final Function UNPARTITION = s -> null;

  @SuppressWarnings("unchecked")
  public static <T> Function<T, StructLike> unpartition() {
    return UNPARTITION;
  }

  private final PartitioningWriter<T, DataWriteResult> writer;
  private final Function<T, StructLike> partitioner;
  private final PartitionSpec spec;
  private final FileIO io;

  public DirectTaskWriter(
      PartitioningWriterFactory<T> partitioningWriterFactory,
      Function<T, StructLike> partitioner, PartitionSpec spec,
      FileIO io) {
    this.writer = partitioningWriterFactory.newDataWriter();
    this.partitioner = partitioner;
    this.spec = spec;
    this.io = io;
  }

  @Override
  public void write(T row) throws IOException {
    StructLike partition = partitioner.apply(row);
    writer.write(row, spec, partition);
  }

  @Override
  public void abort() throws IOException {
    close();

    // clean up files created by this writer
    WriteResult result = writeResult();
    Tasks.foreach(result.dataFiles())
        .throwFailureWhenFinished()
        .noRetry()
        .run(file -> io.deleteFile(file.path().toString()));
  }

  @Override
  public WriteResult complete() throws IOException {
    close();
    return writeResult();
  }

  @Override
  public void close() throws IOException {
    writer.close();
  }

  private WriteResult writeResult() {
    DataWriteResult result = writer.result();

    return WriteResult.builder()
        .addDataFiles(result.dataFiles())
        .build();
  }
}
