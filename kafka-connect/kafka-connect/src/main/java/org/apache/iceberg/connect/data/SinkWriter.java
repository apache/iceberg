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

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;

public class SinkWriter {
  private final IcebergSinkConfig config;
  private final IcebergWriterFactory writerFactory;
  private final Map<String, RecordWriter> writers;
  private final Map<TopicPartition, Offset> sourceOffsets;
  private final List<String> routeFieldPath;
  private final ErrantRecordReporter reporter;
  private final List<Future<Void>> pendingReports;

  public SinkWriter(Catalog catalog, IcebergSinkConfig config) {
    this(catalog, config, null);
  }

  public SinkWriter(Catalog catalog, IcebergSinkConfig config, ErrantRecordReporter reporter) {
    this.config = config;
    this.reporter = reporter;
    this.pendingReports = Lists.newArrayList();
    this.writerFactory = new IcebergWriterFactory(catalog, config);
    this.writers = Maps.newHashMap();
    this.sourceOffsets = Maps.newHashMap();
    String routeField = config.tablesRouteField();
    this.routeFieldPath = routeField == null ? null : Splitter.on('.').splitToList(routeField);
  }

  public void close() {
    writers.values().forEach(RecordWriter::close);
    pendingReports.clear();
  }

  public SinkWriterResult completeWrite() {
    // the offsets move past reported records, so their reports must complete first
    awaitPendingReports();

    List<IcebergWriterResult> writerResults =
        writers.values().stream()
            .flatMap(writer -> writer.complete().stream())
            .collect(Collectors.toList());
    Map<TopicPartition, Offset> offsets = Maps.newHashMap(sourceOffsets);

    writers.clear();
    sourceOffsets.clear();

    return new SinkWriterResult(writerResults, offsets);
  }

  public void save(Collection<SinkRecord> sinkRecords) {
    for (SinkRecord record : sinkRecords) {
      try {
        save(record);
      } catch (DataException e) {
        if (reporter == null) {
          throw e;
        }

        // throws if errors.tolerance does not allow the error
        pendingReports.add(reporter.report(record, e));
      }
    }
  }

  private void awaitPendingReports() {
    try {
      for (Future<Void> report : pendingReports) {
        report.get();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ConnectException("Interrupted while waiting for errant records to be reported", e);
    } catch (ExecutionException e) {
      throw new ConnectException("Failed to report errant records", e.getCause());
    }

    pendingReports.clear();
  }

  private void save(SinkRecord record) {
    // the consumer stores the offsets that corresponds to the next record to consume,
    // so increment the record offset by one
    OffsetDateTime timestamp =
        record.timestamp() == null
            ? null
            : OffsetDateTime.ofInstant(Instant.ofEpochMilli(record.timestamp()), ZoneOffset.UTC);
    // use the original topic and partition to track offsets, as SMTs may have changed
    // record.topic() and record.kafkaPartition() (e.g. RegexRouter). The framework's
    // context.assignment() and consumer offset management use the original values.
    sourceOffsets.put(
        new TopicPartition(record.originalTopic(), record.originalKafkaPartition()),
        new Offset(record.originalKafkaOffset() + 1, timestamp));

    if (config.dynamicTablesEnabled()) {
      routeRecordDynamically(record);
    } else {
      routeRecordStatically(record);
    }
  }

  private void routeRecordStatically(SinkRecord record) {
    if (routeFieldPath == null) {
      // route to all tables
      config
          .tables()
          .forEach(
              tableName -> {
                writerForTable(tableName, record, false).write(record);
              });

    } else {
      String routeValue = extractRouteValue(record.value());
      if (routeValue != null) {
        config
            .tables()
            .forEach(
                tableName -> {
                  Pattern regex = config.tableConfig(tableName).routeRegex();
                  if (regex != null && regex.matcher(routeValue).matches()) {
                    writerForTable(tableName, record, false).write(record);
                  }
                });
      }
    }
  }

  private void routeRecordDynamically(SinkRecord record) {
    Preconditions.checkNotNull(routeFieldPath, "Route field cannot be null with dynamic routing");

    String routeValue = extractRouteValue(record.value());
    if (routeValue != null) {
      String tableName = routeValue.toLowerCase(Locale.ROOT);
      writerForTable(tableName, record, true).write(record);
    }
  }

  private String extractRouteValue(Object recordValue) {
    if (recordValue == null) {
      return null;
    }
    Object routeValue = RecordUtils.extractFromRecordValue(recordValue, routeFieldPath, config);
    return routeValue == null ? null : routeValue.toString();
  }

  private RecordWriter writerForTable(
      String tableName, SinkRecord sample, boolean ignoreMissingTable) {
    return writers.computeIfAbsent(
        tableName, notUsed -> writerFactory.createWriter(tableName, sample, ignoreMissingTable));
  }
}
