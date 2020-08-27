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
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.io.BaseTaskWriter;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

abstract class PartitionedFanoutWriter<T> extends BaseTaskWriter<T> {
  private final Map<PartitionKey, RollingFileWriter> writers = Maps.newHashMap();

  PartitionedFanoutWriter(PartitionSpec spec, FileFormat format, FileAppenderFactory<T> appenderFactory,
                          OutputFileFactory fileFactory, FileIO io, long targetFileSize) {
    super(spec, format, appenderFactory, fileFactory, io, targetFileSize);
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

    RollingFileWriter writer = writers.get(partitionKey);
    if (writer == null) {
      // NOTICE: we need to copy a new partition key here, in case of messing up the keys in writers.
      PartitionKey copiedKey = partitionKey.copy();
      writer = new RollingFileWriter(copiedKey);
      writers.put(copiedKey, writer);
    }

    writer.add(row);
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
}
