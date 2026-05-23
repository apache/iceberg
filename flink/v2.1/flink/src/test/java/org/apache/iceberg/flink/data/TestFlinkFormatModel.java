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
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.BaseFormatModelTests;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.RowDataConverter;
import org.apache.iceberg.flink.TestHelpers;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

public class TestFlinkFormatModel extends BaseFormatModelTests<RowData> {

  @Override
  protected Class<RowData> engineType() {
    return RowData.class;
  }

  @Override
  protected Object engineSchema(Schema schema) {
    return FlinkSchemaUtil.convert(schema);
  }

  @Override
  protected RowData convertToEngine(Record record, Schema schema) {
    return RowDataConverter.convert(schema, record);
  }

  @Override
  protected void assertEquals(Schema schema, List<RowData> expected, List<RowData> actual) {
    TestHelpers.assertRows(actual, expected, FlinkSchemaUtil.convert(schema));
  }

  @Override
  protected Object convertConstantToEngine(Type type, Object value) {
    if (value instanceof PartitionData partitionData) {
      Types.StructType structType = type.asStructType();
      List<Types.NestedField> fields = structType.fields();
      GenericRowData rowData = new GenericRowData(fields.size());
      int sourceSize = partitionData.size();
      for (int i = 0; i < fields.size(); i++) {
        if (i < sourceSize) {
          Object fieldValue = partitionData.get(i, Object.class);
          rowData.setField(i, convertConstantToEngine(fields.get(i).type(), fieldValue));
        } else {
          rowData.setField(i, null);
        }
      }

      return rowData;
    }

    return RowDataUtil.convertConstant(type, value);
  }
}
