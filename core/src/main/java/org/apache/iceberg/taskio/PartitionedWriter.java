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

package org.apache.iceberg.taskio;

import java.io.IOException;
import java.util.Set;
import java.util.function.Function;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PartitionedWriter<T> extends BaseTaskWriter<T> {
  private static final Logger LOG = LoggerFactory.getLogger(PartitionedWriter.class);

  private PartitionKey currentKey = null;
  private WrappedFileAppender currentAppender = null;
  private final Function<T, PartitionKey> partitionKeyGetter;
  private final Set<PartitionKey> completedPartitions = Sets.newHashSet();


  public PartitionedWriter(PartitionSpec spec, FileFormat format, FileAppenderFactory<T> appenderFactory,
                           OutputFileFactory fileFactory, FileIO io, long targetFileSize,
                           Function<T, PartitionKey> partitionKeyGetter) {
    super(spec, format, appenderFactory, fileFactory, io, targetFileSize);
    this.partitionKeyGetter = partitionKeyGetter;
  }

  @Override
  public void write(T row) throws IOException {
    PartitionKey key = partitionKeyGetter.apply(row);

    if (!key.equals(currentKey)) {
      closeCurrentWriter();
      completedPartitions.add(currentKey);

      if (completedPartitions.contains(key)) {
        // if rows are not correctly grouped, detect and fail the write
        PartitionKey existingKey = Iterables.find(completedPartitions, key::equals, null);
        LOG.warn("Duplicate key: {} == {}", existingKey, key);
        throw new IllegalStateException("Already closed files for partition: " + key.toPath());
      }

      currentKey = key.copy();

      createWrappedFileAppender(currentKey, () -> outputFileFactory().newOutputFile(currentKey));
    }

    currentAppender.add(row);
  }

  @Override
  public void abort() throws IOException {
    closeCurrentWriter();

    // clean up files created by this writer
    Tasks.foreach(pollCompleteFiles())
        .throwFailureWhenFinished()
        .noRetry()
        .run(file -> io().deleteFile(file.path().toString()));
  }

  @Override
  public void close() throws IOException {
    closeCurrentWriter();
  }

  private void closeCurrentWriter() throws IOException {
    if (currentAppender != null) {

      // Close the current file appender and put the generated DataFile to completeDataFiles.
      closeWrappedFileAppender(currentAppender);

      // Reset the current appender to be null.
      currentAppender = null;
    }
  }
}
