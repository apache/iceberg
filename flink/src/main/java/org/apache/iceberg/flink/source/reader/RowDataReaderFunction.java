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

package org.apache.iceberg.flink.source.reader;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.source.DataIterator;
import org.apache.iceberg.flink.source.RowDataFileScanTaskReader;
import org.apache.iceberg.flink.source.ScanContext;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;

public class RowDataReaderFunction extends DataIteratorReaderFunction<RowData> {
  private final Table table;
  private final ScanContext scanContext;
  private final Schema readSchema;

  public RowDataReaderFunction(
      Configuration config,
      Table table,
      ScanContext scanContext) {
    super(new ArrayPoolDataIteratorBatcher<>(config, new RowDataRecordFactory(
        FlinkSchemaUtil.convert(readSchema(table, scanContext)))));
    this.table = table;
    this.scanContext = scanContext;
    this.readSchema = readSchema(table, scanContext);
  }

  @Override
  public DataIterator<RowData> createDataIterator(IcebergSourceSplit split) {
    return new DataIterator<>(
        new RowDataFileScanTaskReader(
            table.schema(),
            readSchema,
            scanContext.nameMapping(),
            scanContext.caseSensitive()),
        split.task(),
        table.io(),
        table.encryption());
  }

  private static Schema readSchema(Table table, ScanContext scanContext) {
    return scanContext.project() == null ? table.schema() : scanContext.project();
  }

}
