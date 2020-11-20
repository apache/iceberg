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

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.ContentFileWriter;
import org.apache.iceberg.ContentFileWriterFactory;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.util.Tasks;

public class RollingContentFileWriter<ContentFileT, T> implements Closeable {

  private static final int ROWS_DIVISOR = 1000;
  private final PartitionKey partitionKey;
  private final FileFormat format;
  private final OutputFileFactory fileFactory;
  private final FileIO io;
  private final long targetFileSize;
  private final ContentFileWriterFactory<ContentFileT, T> writerFactory;

  private final WriterResult.Builder resultBuilder;

  private EncryptedOutputFile currentFile = null;
  private ContentFileWriter<ContentFileT, T> currentFileWriter = null;
  private long currentRows = 0;

  public RollingContentFileWriter(PartitionKey partitionKey, FileFormat format,
                                  OutputFileFactory fileFactory, FileIO io, long targetFileSize,
                                  ContentFileWriterFactory<ContentFileT, T> writerFactory) {
    this.partitionKey = partitionKey;
    this.format = format;
    this.fileFactory = fileFactory;
    this.io = io;
    this.targetFileSize = targetFileSize;
    this.writerFactory = writerFactory;

    this.resultBuilder = WriterResult.builder();

    openCurrent();
  }

  CharSequence currentPath() {
    return currentFile.encryptingOutputFile().location();
  }

  long currentRows() {
    return currentRows;
  }

  public void write(T record) {
    this.currentFileWriter.write(record);
    this.currentRows++;

    if (shouldRollToNewFile()) {
      closeCurrent();
      openCurrent();
    }
  }

  public void abort() {
    closeCurrent();

    WriterResult result = resultBuilder.build();

    Tasks.foreach(result.contentFiles())
        .throwFailureWhenFinished()
        .noRetry()
        .run(file -> io.deleteFile(file.path().toString()));
  }

  public WriterResult complete() {
    closeCurrent();

    return resultBuilder.build();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("partitionKey", partitionKey)
        .add("format", format)
        .toString();
  }

  private void openCurrent() {
    if (partitionKey == null) {
      // unpartitioned
      currentFile = fileFactory.newOutputFile();
    } else {
      // partitioned
      currentFile = fileFactory.newOutputFile(partitionKey);
    }
    currentFileWriter = writerFactory.createWriter(partitionKey, currentFile, format);
    currentRows = 0;
  }

  private boolean shouldRollToNewFile() {
    // TODO: ORC file now not support target file size before closed
    return !format.equals(FileFormat.ORC) &&
        currentRows % ROWS_DIVISOR == 0 && currentFileWriter.length() >= targetFileSize;
  }

  protected void beforeClose(ContentFileWriter<ContentFileT, T> writer) {
  }

  private void closeCurrent() {
    if (currentFileWriter != null) {

      // Call the hook before close the current writer.
      beforeClose(currentFileWriter);

      try {
        currentFileWriter.close();
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to close the current file writer: ", e);
      }

      ContentFileT contentFile = currentFileWriter.toContentFile();
      this.currentFileWriter = null;

      if (currentRows == 0L) {
        io.deleteFile(currentFile.encryptingOutputFile());
      } else if (contentFile instanceof ContentFile) {
        resultBuilder.add((ContentFile) contentFile);
      } else {
        throw new RuntimeException(String.format(
            "The newly generated content file must be DataFile or DeleteFile: %s", contentFile));
      }

      this.currentFile = null;
      this.currentRows = 0;
    }
  }

  @Override
  public void close() throws IOException {
    closeCurrent();
  }
}
