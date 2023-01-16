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

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.FileWriterFactory;
import org.apache.iceberg.io.TestWriterMetrics;

public class TestFlinkWriterMetrics extends TestWriterMetrics<RowData> {

  public TestFlinkWriterMetrics(FileFormat fileFormat) {
    super(fileFormat);
  }

  @Override
  protected FileWriterFactory<RowData> newWriterFactory(Table sourceTable) {
    return FlinkFileWriterFactory.builderFor(sourceTable)
        .dataSchema(sourceTable.schema())
        .dataFileFormat(fileFormat)
        .deleteFileFormat(fileFormat)
        .positionDeleteRowSchema(sourceTable.schema())
        .build();
  }

  @Override
  protected RowData toRow(Integer id, String data, boolean boolValue, Long longValue) {
    GenericRowData nested = GenericRowData.of(boolValue, longValue);
    GenericRowData row = GenericRowData.of(id, StringData.fromString(data), nested);
    return row;
  }

  @Override
  public RowData toGenericRow(int value, int repeated) {
    GenericRowData row = new GenericRowData(repeated);
    for (int i = 0; i < repeated; i++) {
      row.setField(i, value);
    }
    return row;
  }
}
