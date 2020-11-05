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
import org.apache.iceberg.ContentFileWriterFactory;
import org.apache.iceberg.FileFormat;

public class UnpartitionedWriter<ContentFileT, T> implements TaskWriter<T> {

  private final RollingContentFileWriter<ContentFileT, T> currentWriter;

  public UnpartitionedWriter(FileFormat format, OutputFileFactory fileFactory, FileIO io, long targetFileSize,
                             ContentFileWriterFactory<ContentFileT, T> writerFactory) {
    currentWriter = new RollingContentFileWriter<>(null, format, fileFactory, io,
        targetFileSize, writerFactory);
  }

  @Override
  public void write(T record) throws IOException {
    currentWriter.add(record);
  }

  @Override
  public void abort() throws IOException {
    currentWriter.abort();
  }

  @Override
  public TaskWriterResult complete() throws IOException {
    return currentWriter.complete();
  }

  @Override
  public void close() throws IOException {
    currentWriter.close();
  }
}
