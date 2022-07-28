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

import java.util.List;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/**
 * A data writer capable of writing to multiple specs and partitions that requires the incoming
 * records to be properly clustered by partition spec and by partition within each spec.
 */
public class ClusteredDataWriter<T> extends ClusteredWriter<T, DataWriteResult> {

  private final FileWriterFactory<T> writerFactory;
  private final OutputFileFactory fileFactory;
  private final FileIO io;
  private final long targetFileSizeInBytes;
  private final List<DataFile> dataFiles;

  public ClusteredDataWriter(
      FileWriterFactory<T> writerFactory,
      OutputFileFactory fileFactory,
      FileIO io,
      long targetFileSizeInBytes) {
    this.writerFactory = writerFactory;
    this.fileFactory = fileFactory;
    this.io = io;
    this.targetFileSizeInBytes = targetFileSizeInBytes;
    this.dataFiles = Lists.newArrayList();
  }

  @Override
  protected FileWriter<T, DataWriteResult> newWriter(PartitionSpec spec, StructLike partition) {
    return new RollingDataWriter<>(
        writerFactory, fileFactory, io, targetFileSizeInBytes, spec, partition);
  }

  @Override
  protected void addResult(DataWriteResult result) {
    dataFiles.addAll(result.dataFiles());
  }

  @Override
  protected DataWriteResult aggregatedResult() {
    return new DataWriteResult(dataFiles);
  }
}
