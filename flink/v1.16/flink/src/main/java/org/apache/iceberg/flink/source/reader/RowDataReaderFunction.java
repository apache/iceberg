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

import static org.apache.iceberg.TableProperties.DEFAULT_NAME_MAPPING;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.source.DataIterator;
import org.apache.iceberg.flink.source.RowDataFileScanTaskReader;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public class RowDataReaderFunction extends DataIteratorReaderFunction<RowData> {
  private final Table table;
  private final Schema readSchema;
  private final boolean caseSensitive;

  public RowDataReaderFunction(
      SerializableTable table,
      ReadableConfig config,
      Schema projectedSchema,
      boolean caseSensitive) {
    super(
        new ArrayPoolDataIteratorBatcher<>(
            config,
            new RowDataRecordFactory(
                FlinkSchemaUtil.convert(readSchema(table.schema(), projectedSchema)))));
    this.table = table;
    this.readSchema = readSchema(table.schema(), projectedSchema);
    this.caseSensitive = caseSensitive;
  }

  @Override
  public DataIterator<RowData> createDataIterator(IcebergSourceSplit split) {
    return new DataIterator<>(
        table,
        new RowDataFileScanTaskReader(
            table.schema(),
            readSchema,
            table.properties().get(DEFAULT_NAME_MAPPING),
            caseSensitive),
        split.task());
  }

  private static Schema readSchema(Schema tableSchema, Schema projectedSchema) {
    Preconditions.checkNotNull(tableSchema, "Table schema can't be null");
    return projectedSchema == null ? tableSchema : projectedSchema;
  }
}
