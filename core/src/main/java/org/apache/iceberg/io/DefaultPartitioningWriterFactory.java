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

import org.apache.iceberg.FileFormat;
import org.apache.iceberg.deletes.PositionDelete;

class DefaultPartitioningWriterFactory<T> implements PartitioningWriterFactory<T> {
  private final FileWriterFactory<T> writerFactory;
  private final OutputFileFactory fileFactory;
  private final FileIO io;
  private final FileFormat fileFormat;
  private final long targetFileSizeInBytes;
  private final Type type;

  DefaultPartitioningWriterFactory(
      FileWriterFactory<T> writerFactory, OutputFileFactory fileFactory,
      FileIO io, FileFormat fileFormat, long targetFileSizeInBytes, Type type) {
    this.writerFactory = writerFactory;
    this.fileFactory = fileFactory;
    this.io = io;
    this.fileFormat = fileFormat;
    this.targetFileSizeInBytes = targetFileSizeInBytes;
    this.type = type;
  }

  @Override
  public PartitioningWriter<T, DataWriteResult> newDataWriter() {
    return type == Type.CLUSTERED ?
        new ClusteredDataWriter<>(writerFactory, fileFactory, io, fileFormat, targetFileSizeInBytes) :
        new FanoutDataWriter<>(writerFactory, fileFactory, io, fileFormat, targetFileSizeInBytes);
  }

  @Override
  public PartitioningWriter<T, DeleteWriteResult> newEqualityDeleteWriter() {
    return type == Type.CLUSTERED ?
        new ClusteredEqualityDeleteWriter<>(writerFactory, fileFactory, io, fileFormat, targetFileSizeInBytes) :
        new FanoutEqualityDeleteWriter<>(writerFactory, fileFactory, io, fileFormat, targetFileSizeInBytes);
  }

  @Override
  public PartitioningWriter<PositionDelete<T>, DeleteWriteResult> newPositionDeleteWriter() {
    return type == Type.CLUSTERED ?
        new ClusteredPositionDeleteWriter<>(writerFactory, fileFactory, io, fileFormat, targetFileSizeInBytes) :
        new FanoutPositionDeleteWriter<>(writerFactory, fileFactory, io, fileFormat, targetFileSizeInBytes);
  }

  enum Type {
    CLUSTERED,
    FANOUT,
  }
}
