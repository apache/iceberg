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
import java.util.Set;
import org.apache.iceberg.ContentFileWriterFactory;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class PartitionedWriter<ContentFileT, T> implements TaskWriter<T> {
  private static final Logger LOG = LoggerFactory.getLogger(PartitionedWriter.class);

  private final FileFormat format;
  private final OutputFileFactory fileFactory;
  private final FileIO io;
  private final long targetFileSize;
  private final ContentFileWriterFactory<ContentFileT, T> writerFactory;
  private final WriterResult.Builder resultBuilder;

  private final Set<PartitionKey> completedPartitions = Sets.newHashSet();

  private PartitionKey currentKey = null;
  private RollingContentFileWriter<ContentFileT, T> currentWriter = null;

  public PartitionedWriter(FileFormat format, OutputFileFactory fileFactory, FileIO io, long targetFileSize,
                           ContentFileWriterFactory<ContentFileT, T> writerFactory) {
    this.format = format;
    this.fileFactory = fileFactory;
    this.io = io;
    this.targetFileSize = targetFileSize;
    this.writerFactory = writerFactory;
    this.resultBuilder = WriterResult.builder();
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
    PartitionKey key = partition(row);

    if (!key.equals(currentKey)) {
      if (currentKey != null) {
        // if the key is null, there was no previous current key and current writer.
        resultBuilder.add(currentWriter.complete());
        completedPartitions.add(currentKey);
      }

      if (completedPartitions.contains(key)) {
        // if rows are not correctly grouped, detect and fail the write
        PartitionKey existingKey = Iterables.find(completedPartitions, key::equals, null);
        LOG.warn("Duplicate key: {} == {}", existingKey, key);
        throw new IllegalStateException("Already closed files for partition: " + key.toPath());
      }

      currentKey = key.copy();
      currentWriter = new RollingContentFileWriter<>(currentKey, format,
          fileFactory, io, targetFileSize, writerFactory);
    }

    currentWriter.write(row);
  }

  @Override
  public void close() throws IOException {
    if (currentWriter != null) {
      currentWriter.close();
    }
  }

  @Override
  public void abort() throws IOException {
    close();

    if (currentWriter != null) {
      currentWriter.abort();
      currentWriter = null;
    }

    Tasks.foreach(resultBuilder.build().contentFiles())
        .throwFailureWhenFinished()
        .noRetry()
        .run(file -> io.deleteFile(file.path().toString()));
  }

  @Override
  public WriterResult complete() throws IOException {
    close();

    if (currentWriter != null) {
      resultBuilder.add(currentWriter.complete());
      currentWriter = null;
    }

    return resultBuilder.build();
  }
}
