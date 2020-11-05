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

package org.apache.iceberg.flink.sink;

import java.io.IOException;
import java.util.Map;
import org.apache.iceberg.ContentFileWriterFactory;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.RollingContentFileWriter;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriterResult;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class PartitionedFanoutWriter<ContentFileT, T> implements TaskWriter<T> {

  private static final Logger LOG = LoggerFactory.getLogger(PartitionedFanoutWriter.class);

  private final FileFormat format;
  private final OutputFileFactory fileFactory;
  private final FileIO io;
  private final long targetFileSize;
  private final ContentFileWriterFactory<ContentFileT, T> writerFactory;

  private final Map<PartitionKey, RollingContentFileWriter<ContentFileT, T>> writers = Maps.newHashMap();

  PartitionedFanoutWriter(FileFormat format, OutputFileFactory fileFactory, FileIO io, long targetFileSize,
                          ContentFileWriterFactory<ContentFileT, T> writerFactory) {
    this.format = format;
    this.fileFactory = fileFactory;
    this.io = io;
    this.targetFileSize = targetFileSize;
    this.writerFactory = writerFactory;
  }

  /**
   * Create a PartitionKey from the values in row.
   * <p>
   * Any PartitionKey returned by this method can be reused by the implementation.
   *
   * @param row a data row
   */
  protected abstract PartitionKey partition(T row);

  @Override
  public void write(T row) throws IOException {
    PartitionKey partitionKey = partition(row);

    RollingContentFileWriter<ContentFileT, T> writer = writers.get(partitionKey);
    if (writer == null) {
      // NOTICE: we need to copy a new partition key here, in case of messing up the keys in writers.
      PartitionKey copiedKey = partitionKey.copy();
      writer = new RollingContentFileWriter<>(copiedKey, format, fileFactory, io, targetFileSize, writerFactory);
      writers.put(copiedKey, writer);
    }

    writer.write(row);
  }

  @Override
  public void close() throws IOException {
    if (!writers.isEmpty()) {
      for (PartitionKey key : writers.keySet()) {
        writers.get(key).close();
      }
      writers.clear();
    }
  }

  @Override
  public void abort() {
    for (RollingContentFileWriter<ContentFileT, T> writer : writers.values()) {
      try {
        writer.abort();
      } catch (IOException e) {
        LOG.warn("Failed to abort the writer {} because: ", writer, e);
      }
    }
    writers.clear();
  }

  @Override
  public WriterResult complete() throws IOException {
    WriterResult.Builder builder = WriterResult.builder();

    for (RollingContentFileWriter<ContentFileT, T> writer : writers.values()) {
      builder.add(writer.complete());
    }
    writers.clear();

    return builder.build();
  }
}
