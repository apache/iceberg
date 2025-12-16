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
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SinkWriter {

  private static final Logger LOG = LoggerFactory.getLogger(SinkWriter.class);

  private final IcebergSinkConfig config;
  private final IcebergWriterFactory writerFactory;
  private final Map<String, RecordWriter> writers;
  private final Map<TopicPartition, Offset> sourceOffsets;
  private ErrantRecordReporter reporter;

  public SinkWriter(Catalog catalog, IcebergSinkConfig config) {
    this.config = config;
    this.writerFactory = new IcebergWriterFactory(catalog, config);
    this.writers = Maps.newHashMap();
    this.sourceOffsets = Maps.newHashMap();
  }

  public void setReporter(ErrantRecordReporter reporter) {
    this.reporter = reporter;
  }

  public void close() {
    writers.values().forEach(RecordWriter::close);
  }

  public SinkWriterResult completeWrite() {
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
        this.save(record);
      } catch (DataException ex) {
        if (this.reporter != null) {
          this.reporter.report(record, ex);
        }
        if (this.config.errorTolerance().equalsIgnoreCase(ErrorTolerance.ALL.toString())) {
          LOG.error(
              "Data exception encountered while saving record but tolerated due to error tolerance settings. "
                  + "To change this behavior, set 'errors.tolerance' to 'none':",
              ex);
        } else {
          throw ex;
        }
      }
    }
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

    if (config.dynamicTablesEnabled()) {
      routeRecordDynamically(record);
    } else {
      routeRecordStatically(record);
    }
  }

  private void routeRecordStatically(SinkRecord record) {
    String routeField = config.tablesRouteField();

    if (routeField == null) {
      // route to all tables
      config
          .tables()
          .forEach(
              tableName -> {
                writerForTable(tableName, record, false).write(record);
              });

    } else {
      String routeValue = extractRouteValue(record.value(), routeField);
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
    String routeField = config.tablesRouteField();
    Preconditions.checkNotNull(routeField, "Route field cannot be null with dynamic routing");

    String routeValue = extractRouteValue(record.value(), routeField);
    if (routeValue != null) {
      String tableName = routeValue.toLowerCase(Locale.ROOT);
      writerForTable(tableName, record, true).write(record);
    }
  }

  private String extractRouteValue(Object recordValue, String routeField) {
    if (recordValue == null) {
      return null;
    }
    Object routeValue = RecordUtils.extractFromRecordValue(recordValue, routeField);
    return routeValue == null ? null : routeValue.toString();
  }

  private RecordWriter writerForTable(
      String tableName, SinkRecord sample, boolean ignoreMissingTable) {
    return writers.computeIfAbsent(
        tableName, notUsed -> writerFactory.createWriter(tableName, sample, ignoreMissingTable));
  }
}
