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
package org.apache.iceberg.connect.data;

import java.lang.reflect.InvocationTargetException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;

public class SinkWriter {
  private final Map<TopicPartition, Offset> sourceOffsets;
  private final RecordRouter router;

  public SinkWriter(Catalog catalog, IcebergSinkConfig config) {
    this.sourceOffsets = Maps.newHashMap();
    if (config.dynamicTablesEnabled()) {
      router = new RecordRouter.DynamicRecordRouter(catalog, config);
    } else if (config.tablesRouteWith() == null && config.tablesRouteField() != null) {
      router = new RecordRouter.StaticRecordRouter(catalog, config);
    } else if (config.tablesRouteWith() != null) {
      try {
        router =
            config
                .tablesRouteWith()
                .getDeclaredConstructor(Catalog.class, IcebergSinkConfig.class)
                .newInstance(catalog, config);
      } catch (NoSuchMethodException
          | InstantiationException
          | IllegalAccessException
          | InvocationTargetException e) {
        throw new IllegalArgumentException(
            "Cannot create router from iceberg.tables.route-with", e);
      }
    } else {
      router = new RecordRouter.AllTablesRecordRouter(catalog, config);
    }
  }

  public void close() {
    router.close();
  }

  public SinkWriterResult completeWrite() {
    List<IcebergWriterResult> writerResults = router.completeWrite();
    Map<TopicPartition, Offset> offsets = Maps.newHashMap(sourceOffsets);

    router.clearWriters();
    sourceOffsets.clear();

    return new SinkWriterResult(writerResults, offsets);
  }

  public void save(Collection<SinkRecord> sinkRecords) {
    sinkRecords.forEach(this::save);
  }

  private void save(SinkRecord record) {
    // the consumer stores the offsets that corresponds to the next record to consume,
    // so increment the record offset by one
    OffsetDateTime timestamp =
        record.timestamp() == null
            ? null
            : OffsetDateTime.ofInstant(Instant.ofEpochMilli(record.timestamp()), ZoneOffset.UTC);
    sourceOffsets.put(
        new TopicPartition(record.topic(), record.kafkaPartition()),
        new Offset(record.kafkaOffset() + 1, timestamp));

    router.routeRecord(record);
  }
}
