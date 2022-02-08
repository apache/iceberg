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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.Tasks;

public abstract class PartitionedFanoutWriter<T> extends BaseTaskWriter<T> {
  private Cache<PartitionKey, RollingFileWriter> writers;

  protected PartitionedFanoutWriter(PartitionSpec spec, FileFormat format, FileAppenderFactory<T> appenderFactory,
                                    OutputFileFactory fileFactory, FileIO io, long targetFileSize) {
    super(spec, format, appenderFactory, fileFactory, io, targetFileSize);
    initWritersCache(ImmutableMap.of());
  }

  protected PartitionedFanoutWriter(PartitionSpec spec, FileFormat format, FileAppenderFactory<T> appenderFactory,
                                    OutputFileFactory fileFactory, FileIO io,
                                    long targetFileSize, Map<String, String> properties) {
    super(spec, format, appenderFactory, fileFactory, io, targetFileSize);
    initWritersCache(properties);
  }

  private void initWritersCache(Map<String, String> properties) {
    if (writers == null) {
      int writersCacheSize = PropertyUtil.propertyAsInt(
          properties,
          TableProperties.PARTITIONED_FANOUT_WRITERS_CACHE_SIZE,
          TableProperties.PARTITIONED_FANOUT_WRITERS_CACHE_SIZE_DEFAULT);
      long evictionTimeout = PropertyUtil.propertyAsLong(
          properties,
          TableProperties.PARTITIONED_FANOUT_WRITERS_CACHE_EVICT_MS,
          TableProperties.PARTITIONED_FANOUT_WRITERS_CACHE_EVICT_MS_DEFAULT);
      writers = Caffeine.newBuilder()
          .maximumSize(writersCacheSize)
          .expireAfterAccess(evictionTimeout, TimeUnit.MILLISECONDS)
          .removalListener((key, value, cause) -> {
            try {
              ((RollingFileWriter) value).close();
            } catch (IOException e) {
              throw new UncheckedIOException("Failed to close rolling file writer", e);
            }
          })
          .build();
    }
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

    RollingFileWriter writer = writers.getIfPresent(partitionKey);
    if (writer == null) {
      // NOTICE: we need to copy a new partition key here, in case of messing up the keys in writers.
      PartitionKey copiedKey = partitionKey.copy();
      writer = new RollingFileWriter(copiedKey);
      writers.put(copiedKey, writer);
    }

    writer.write(row);
  }

  @Override
  public void close() throws IOException {
    ConcurrentMap<PartitionKey, RollingFileWriter> writersMap = writers.asMap();
    if (!writersMap.isEmpty()) {
      // close all remaining rolling file writers
      try {
        Tasks.foreach(writersMap.values())
            .throwFailureWhenFinished()
            .noRetry()
            .run(RollingFileWriter::close, IOException.class);
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to close rolling file writer", e);
      }
    }
    writers.invalidateAll();
  }
}
