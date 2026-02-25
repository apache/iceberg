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
package org.apache.iceberg.flink.data;

import java.util.List;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TestBase;
import org.apache.iceberg.data.TestBaseFormatModel;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TestHelpers;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;

public class TestFlinkFormatModels extends TestBaseFormatModel<RowData, RowData> {

  @Override
  protected Class<RowData> writeType() {
    return RowData.class;
  }

  @Override
  protected Class<RowData> readType() {
    return RowData.class;
  }

  @Override
  protected Object writeEngineSchema(Schema schema) {
    return FlinkSchemaUtil.convert(schema);
  }

  @Override
  protected Object readEngineSchema(Schema schema) {
    return FlinkSchemaUtil.convert(schema);
  }

  @Override
  protected List<RowData> testRecords() {
    return Lists.newArrayList(RandomRowData.generate(TestBase.SCHEMA, 10, 1L));
  }

  @Override
  protected void assertEquals(
      Types.StructType struct, List<RowData> expected, List<RowData> actual) {
    Schema schema = new Schema(struct.fields());
    RowType rowType = FlinkSchemaUtil.convert(schema);
    TestHelpers.assertRows(actual, expected, rowType);
  }

  @Override
  protected List<RowData> expectedPositionDeletes(Schema schema) {
    return ImmutableList.of(
        toPositionDeleteRow("data-file-1.parquet", 0L),
        toPositionDeleteRow("data-file-1.parquet", 1L));
  }

  private static RowData toPositionDeleteRow(String filePath, long pos) {
    GenericRowData row = new GenericRowData(2);
    row.setField(0, StringData.fromString(filePath));
    row.setField(1, pos);
    return row;
  }
}
