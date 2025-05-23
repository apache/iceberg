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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.handler.DlqReporter;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class IcebergWriter implements RecordWriter {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergWriter.class);

  private final Table table;
  private final String tableName;
  private final IcebergSinkConfig config;
  private final List<IcebergWriterResult> writerResults;

  private RecordConverter recordConverter;
  private TaskWriter<Record> writer;
  private DlqReporter dlqReporter;

  IcebergWriter(Table table, String tableName, IcebergSinkConfig config) {
    this.table = table;
    this.tableName = tableName;
    this.config = config;
    this.writerResults = Lists.newArrayList();

    if (!config.errorDeadLetterQueueTopicNameConfig().isEmpty()) {
      this.dlqReporter = new DlqReporter(config, config.errorDeadLetterQueueTopicNameConfig());
    }
    initNewWriter();
  }

  private void initNewWriter() {
    this.writer = RecordUtils.createTableWriter(table, tableName, config);
    this.recordConverter = new RecordConverter(table, config);
  }

  @Override
  public void write(SinkRecord record) throws DataException {
    try {
      // ignore tombstones...
      if (record.value() != null) {
        Record row = convertToRow(record);
        writer.write(row);
      }
    } catch (Exception e) {
      String recordData = "";
      if (this.config.errorLogIncludeMessages()) {
        recordData = String.format(", record: %s", record.value().toString());
      }
      DataException ex =
          new DataException(
              String.format(
                  Locale.ROOT,
                  "topic: %s, partition, %d, offset: %d %s",
                  record.topic(),
                  record.kafkaPartition(),
                  record.kafkaOffset(),
                  recordData),
              e);
      if (this.config.errorTolerance().equalsIgnoreCase(ErrorTolerance.ALL.toString())) {
        if (this.dlqReporter != null) {
          this.dlqReporter.send(record);
        }
        LOG.error("An error occurred converting record...", ex);
      } else {
        throw ex;
      }
    }
  }

  private Record convertToRow(SinkRecord record) {
    if (!config.evolveSchemaEnabled()) {
      return recordConverter.convert(record.value());
    }

    SchemaUpdate.Consumer updates = new SchemaUpdate.Consumer();
    Record row = recordConverter.convert(record.value(), updates);

    if (!updates.empty()) {
      // complete the current file
      flush();
      // apply the schema updates, this will refresh the table
      SchemaUtils.applySchemaUpdates(table, updates);
      // initialize a new writer with the new schema
      initNewWriter();
      // convert the row again, this time using the new table schema
      row = recordConverter.convert(record.value(), null);
    }

    return row;
  }

  private void flush() {
    WriteResult writeResult;
    try {
      writeResult = writer.complete();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    writerResults.add(
        new IcebergWriterResult(
            TableIdentifier.parse(tableName),
            Arrays.asList(writeResult.dataFiles()),
            Arrays.asList(writeResult.deleteFiles()),
            table.spec().partitionType()));
  }

  @Override
  public List<IcebergWriterResult> complete() {
    flush();

    List<IcebergWriterResult> result = Lists.newArrayList(writerResults);
    writerResults.clear();

    return result;
  }

  @Override
  public void close() {
    try {
      writer.close();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
