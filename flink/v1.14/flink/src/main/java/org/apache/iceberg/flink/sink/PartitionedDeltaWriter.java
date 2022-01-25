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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.Tasks;

class PartitionedDeltaWriter extends BaseDeltaTaskWriter {

  private final PartitionKey partitionKey;

  private Cache<PartitionKey, RowDataDeltaWriter> writers;

  PartitionedDeltaWriter(PartitionSpec spec,
                         FileFormat format,
                         FileAppenderFactory<RowData> appenderFactory,
                         OutputFileFactory fileFactory,
                         FileIO io,
                         long targetFileSize,
                         Schema schema,
                         RowType flinkSchema,
                         List<Integer> equalityFieldIds,
                         boolean upsert,
                         Map<String, String> properties) {
    super(spec, format, appenderFactory, fileFactory, io, targetFileSize, schema, flinkSchema, equalityFieldIds,
        upsert);
    this.partitionKey = new PartitionKey(spec, schema);
    int writersCacheSize = PropertyUtil.propertyAsInt(
        properties,
        TableProperties.PARTITIONED_DELTA_WRITERS_CACHE_SIZE,
        TableProperties.PARTITIONED_DELTA_WRITERS_CACHE_SIZE_DEFAULT);
    long evictionTimeout = PropertyUtil.propertyAsLong(
        properties,
        TableProperties.PARTITIONED_DELTA_WRITERS_CACHE_EVICT_MS,
        TableProperties.PARTITIONED_DELTA_WRITERS_CACHE_EVICT_MS_DEFAULT);
    initWritersCache(writersCacheSize, evictionTimeout);
  }

  private synchronized void initWritersCache(int writersCacheSize, long evictionTimeout) {
    if (writers == null) {
      writers = Caffeine.newBuilder()
          .maximumSize(writersCacheSize)
          .expireAfterAccess(evictionTimeout, TimeUnit.MILLISECONDS)
          .removalListener((key, value, cause) -> {
            try {
              ((RowDataDeltaWriter) value).close();
            } catch (IOException e) {
              throw new UncheckedIOException("Failed to close rolling file writer", e);
            }
          })
          .build();
    }
  }

  @Override
  RowDataDeltaWriter route(RowData row) {
    partitionKey.partition(wrapper().wrap(row));

    RowDataDeltaWriter writer = writers.getIfPresent(partitionKey);
    if (writer == null) {
      // NOTICE: we need to copy a new partition key here, in case of messing up the keys in writers.
      PartitionKey copiedKey = partitionKey.copy();
      writer = new RowDataDeltaWriter(copiedKey);
      writers.put(copiedKey, writer);
    }

    return writer;
  }

  @Override
  public void close() {
    ConcurrentMap<PartitionKey, RowDataDeltaWriter> writersMap = writers.asMap();
    if (writersMap.size() > 0) {
      try {
        Tasks.foreach(writersMap.values())
            .throwFailureWhenFinished()
            .noRetry()
            .run(RowDataDeltaWriter::close, IOException.class);
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to close equality delta writer", e);
      }
    }
    writers.invalidateAll();
  }
}
