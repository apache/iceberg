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
import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

public class PartitionedFanoutWriter<T> extends BaseTaskWriter<T> {
  private final Function<T, PartitionKey> keyGetter;
  private final Map<PartitionKey, WrappedFileAppender> writers = Maps.newHashMap();

  public PartitionedFanoutWriter(PartitionSpec spec, FileFormat format, FileAppenderFactory<T> appenderFactory,
                                 OutputFileFactory fileFactory, FileIO io, long targetFileSize,
                                 Function<T, PartitionKey> keyGetter) {
    super(spec, format, appenderFactory, fileFactory, io, targetFileSize);
    this.keyGetter = keyGetter;
  }

  @Override
  public void write(T row) throws IOException {
    PartitionKey partitionKey = keyGetter.apply(row);

    WrappedFileAppender writer = writers.get(partitionKey);
    if (writer == null) {
      // NOTICE: we need to copy a new partition key here, in case of messing up the keys in writers.
      PartitionKey copiedKey = partitionKey.copy();
      writer = createWrappedFileAppender(copiedKey, () -> outputFileFactory().newOutputFile(partitionKey));
      writers.put(copiedKey, writer);
    }
    writer.add(row);

    // Close the writer if reach the target file size.
    if (writer.shouldRollToNewFile()) {
      writer.close();
      writers.remove(partitionKey);
    }
  }

  @Override
  public void close() throws IOException {
    if (!writers.isEmpty()) {
      Iterator<WrappedFileAppender> iterator = writers.values().iterator();
      while (iterator.hasNext()) {
        iterator.next().close();
        // Remove from the writers after closed.
        iterator.remove();
      }
    }
  }
}
