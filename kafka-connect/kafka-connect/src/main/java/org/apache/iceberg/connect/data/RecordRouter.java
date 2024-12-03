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

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * Abstract class for routing records to tables.
 *
 * <p>This class should be extended to create custom routing of records to tables.
 */
public abstract class RecordRouter {

  private final IcebergWriterFactory writerFactory;
  private final IcebergSinkConfig config;
  private final Map<String, RecordWriter> writers = Maps.newHashMap();

  public RecordRouter(Catalog catalog, IcebergSinkConfig config) {
    this.config = config;
    this.writerFactory = new IcebergWriterFactory(catalog, config);
  }

  /**
   * Method to route a record to one or more tables.
   *
   * <p>Implementations must call {@link #writeToTable(String, SinkRecord, boolean)} from this
   * method to write the record to a table.
   *
   * @param record The Kafka record to route.
   */
  public abstract void routeRecord(SinkRecord record);

  /** Get the configuration passed to the Kafka connect plugin. */
  public IcebergSinkConfig config() {
    return config;
  }

  /**
   * Write the record to the table
   *
   * @param tableName The name of the table to write the record to.
   * @param record The record to write to the table.
   * @param ignoreMissingTable If true, missing tables are ignored and no error is thrown.
   */
  public void writeToTable(String tableName, SinkRecord record, boolean ignoreMissingTable) {
    writers
        .computeIfAbsent(
            tableName, notUsed -> writerFactory.createWriter(tableName, record, ignoreMissingTable))
        .write(record);
  }

  /**
   * Extract the value of a field from the record as a string.
   *
   * @param record The Kafka record to extract the value from.
   * @param field The name of the field to extract.
   * @return The value of the field in the record as a string.
   */
  public String extractFieldFromRecordValue(SinkRecord record, String field) {
    if (record.value() == null) {
      return null;
    }
    Object value = RecordUtils.extractFromRecordValue(record.value(), field);
    return value == null ? null : value.toString();
  }

  void close() {
    writers.values().forEach(RecordWriter::close);
  }

  List<IcebergWriterResult> completeWrite() {
    return writers.values().stream()
        .flatMap(writer -> writer.complete().stream())
        .collect(Collectors.toList());
  }

  void clearWriters() {
    writers.clear();
  }

  /** Route record to all the tables */
  public static class AllTablesRecordRouter extends RecordRouter {
    public AllTablesRecordRouter(Catalog catalog, IcebergSinkConfig config) {
      super(catalog, config);
    }

    @Override
    public void routeRecord(SinkRecord record) {
      config().tables().forEach(tableName -> writeToTable(tableName, record, false));
    }
  }

  /** Route records to tables based on a regex that matches the value of a field in the data. */
  public static class StaticRecordRouter extends RecordRouter {
    private final String routeField;
    private final Map<String, Pattern> tablePatterns = Maps.newHashMap();

    public StaticRecordRouter(Catalog catalog, IcebergSinkConfig config) {
      super(catalog, config);
      this.routeField = config.tablesRouteField();
      Preconditions.checkNotNull(routeField, "Route field cannot be null with static routing");
      config
          .tables()
          .forEach(
              tableName ->
                  tablePatterns.put(tableName, config.tableConfig(tableName).routeRegex()));
    }

    @Override
    public void routeRecord(SinkRecord record) {
      String routeValue = extractFieldFromRecordValue(record, routeField);
      if (routeValue != null) {
        tablePatterns.forEach(
            (tableName, tablePattern) -> {
              if (tablePattern != null && tablePattern.matcher(routeValue).matches()) {
                writeToTable(tableName, record, false);
              }
            });
      }
    }
  }

  /** Route records to the table specified in a field in the data. */
  public static class DynamicRecordRouter extends RecordRouter {
    private final String routeField;

    public DynamicRecordRouter(Catalog catalog, IcebergSinkConfig config) {
      super(catalog, config);
      routeField = config.tablesRouteField();
      Preconditions.checkNotNull(routeField, "Route field cannot be null with dynamic routing");
    }

    @Override
    public void routeRecord(SinkRecord record) {
      String routeValue = extractFieldFromRecordValue(record, routeField);
      if (routeValue != null) {
        String tableName = routeValue.toLowerCase(Locale.ROOT);
        writeToTable(tableName, record, true);
      }
    }
  }

  /** Route records to tables using a regex match the topic of the record. */
  public static class TopicRecordRouter extends RecordRouter {
    private final Map<String, List<String>> topicTables = Maps.newHashMap();

    public TopicRecordRouter(Catalog catalog, IcebergSinkConfig config) {
      super(catalog, config);
    }

    private List<String> tablesForTopic(String topic) {
      List<String> tables = Lists.newArrayList();
      config()
          .tables()
          .forEach(
              tableName -> {
                Pattern tableRegex = config().tableConfig(tableName).routeRegex();
                if (tableRegex != null && tableRegex.matcher(topic).matches()) {
                  tables.add(tableName);
                }
              });
      return tables;
    }

    @Override
    public void routeRecord(SinkRecord record) {
      topicTables
          .computeIfAbsent(record.topic(), this::tablesForTopic)
          .forEach(tableName -> writeToTable(tableName, record, false));
    }
  }
}
