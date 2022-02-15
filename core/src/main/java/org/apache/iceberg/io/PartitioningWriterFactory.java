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
import org.apache.iceberg.io.DefaultPartitioningWriterFactory.Type;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Factory to create a new {@link PartitioningWriter}
 */
public interface PartitioningWriterFactory<T> {
  PartitioningWriter<T, DataWriteResult> newDataWriter();

  PartitioningWriter<T, DeleteWriteResult> newEqualityDeleteWriter();

  PartitioningWriter<PositionDelete<T>, DeleteWriteResult> newPositionDeleteWriter();

  static <T> Builder<T> builder(FileWriterFactory<T> writerFactory) {
    return new Builder<>(writerFactory);
  }

  class Builder<T> {
    private final FileWriterFactory<T> writerFactory;
    private OutputFileFactory fileFactory = null;
    private FileIO io = null;
    private FileFormat fileFormat = null;
    private long targetFileSizeInBytes = 0;

    public Builder(FileWriterFactory<T> writerFactory) {
      this.writerFactory = writerFactory;
    }

    private void checkArguments() {
      Preconditions.checkArgument(writerFactory != null, "writerFactory is required non-null");
      Preconditions.checkArgument(fileFactory != null, "fileFactory is required non-null");
      Preconditions.checkArgument(io != null, "io is required non-null");
      Preconditions.checkArgument(fileFormat != null, "fileFormat is required non-null");
    }

    public PartitioningWriterFactory<T> buildForClusteredPartition() {
      checkArguments();
      return new DefaultPartitioningWriterFactory<>(writerFactory, fileFactory, io, fileFormat,
          targetFileSizeInBytes, Type.CLUSTERED);
    }

    public PartitioningWriterFactory<T> buildForFanoutPartition() {
      checkArguments();
      return new DefaultPartitioningWriterFactory<>(writerFactory, fileFactory, io, fileFormat,
          targetFileSizeInBytes, Type.FANOUT);
    }

    public Builder<T> fileFactory(OutputFileFactory newFileFactory) {
      this.fileFactory = newFileFactory;
      return this;
    }

    public Builder<T> io(FileIO newFileIO) {
      this.io = newFileIO;
      return this;
    }

    public Builder<T> fileFormat(FileFormat newFileFormat) {
      this.fileFormat = newFileFormat;
      return this;
    }

    public Builder<T> targetFileSizeInBytes(long newTargetFileSizeInBytes) {
      this.targetFileSizeInBytes = newTargetFileSizeInBytes;
      return this;
    }
  }
}
