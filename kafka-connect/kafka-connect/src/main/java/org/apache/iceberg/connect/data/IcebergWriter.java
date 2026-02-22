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
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.events.TableReference;
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
  private final TableReference tableReference;
  private final IcebergSinkConfig config;
  private final List<IcebergWriterResult> writerResults;

  private RecordConverter recordConverter;
  private TaskWriter<Record> writer;

  IcebergWriter(Table table, TableReference tableReference, IcebergSinkConfig config) {
    this.table = table;
    this.tableReference = tableReference;
    this.config = config;
    this.writerResults = Lists.newArrayList();
    initNewWriter();
  }

  private void initNewWriter() {
    this.writer = RecordUtils.createTableWriter(table, tableReference, config);
    this.recordConverter = new RecordConverter(table, config);
  }

  @Override
  public void write(SinkRecord record) {
    try {
      // ignore tombstones...
      if (record.value() != null) {
        Record row = convertToRow(record);
        writer.write(row);
      }
    } catch (Exception e) {
      throw new DataException(
          String.format(
              Locale.ROOT,
              "An error occurred converting record, topic: %s, partition, %d, offset: %d",
              record.topic(),
              record.kafkaPartition(),
              record.kafkaOffset()),
          e);
    }
  }

  private Record convertToRow(SinkRecord record) {
    if (!config.evolveSchemaEnabled()) {
      return recordConverter.convert(record);
    }

    SchemaUpdate.Consumer updates = new SchemaUpdate.Consumer();
    recordConverter.evolveSchema(record, updates);

    if (!updates.empty()) {
      try {
        // complete the current file
        flush();

        // apply the schema updates, this will refresh the table
        SchemaUtils.applySchemaUpdates(table, updates);
        LOG.info(
            "Schema evolution on table {} caused by record at topic: {}, partition: {}, offset: {}",
            table.name(),
            record.topic(),
            record.kafkaPartition(),
            record.kafkaOffset());
      } catch (Exception e) {
        LOG.error(
            "Schema updates for table {} not applied by record at topic: {}, partition: {}, offset: {} because {}. Data will still be written to table",
            table.name(),
            record.topic(),
            record.kafkaPartition(),
            record.kafkaOffset(),
            e.getMessage(),
            e);
      } finally {
        // initialize a new writer with the latest schema - in case any other task has already
        // applied the schema updates
        initNewWriter();
      }
    }

    // convert the row, this time new table schema will be used
    return recordConverter.convert(record);
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
            tableReference,
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
