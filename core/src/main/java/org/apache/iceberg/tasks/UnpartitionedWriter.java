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

package org.apache.iceberg.tasks;

import java.io.IOException;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.io.FileIO;

public class UnpartitionedWriter<T> extends BaseTaskWriter<T> {

  private WrappedFileAppender currentAppender = null;

  public UnpartitionedWriter(PartitionSpec spec, FileFormat format, FileAppenderFactory<T> appenderFactory,
                             OutputFileFactory fileFactory, FileIO io, long targetFileSize) {
    super(spec, format, appenderFactory, fileFactory, io, targetFileSize);
  }

  @Override
  public void write(T record) throws IOException {
    if (currentAppender == null) {
      currentAppender = createWrappedFileAppender(null, outputFileFactory()::newOutputFile);
    }
    currentAppender.add(record);

    // Close the writer if reach the target file size.
    if (currentAppender.shouldRollToNewFile()) {
      closeCurrent();
    }
  }

  @Override
  public void close() throws IOException {
    closeCurrent();
  }

  private void closeCurrent() throws IOException {
    if (currentAppender != null) {

      // Close the current file appender and put the generated DataFile to completeDataFiles.
      currentAppender.close();

      // Reset the current appender to be null.
      currentAppender = null;
    }
  }
}
