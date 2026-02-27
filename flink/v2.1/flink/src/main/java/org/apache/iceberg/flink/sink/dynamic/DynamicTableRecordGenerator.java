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
package org.apache.iceberg.flink.sink.dynamic;

import java.util.List;
import java.util.Map;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/**
 * Abstract base class for SQL-based dynamic record generators. Users will extend this class to
 * create a DynamicRecord from RowData.
 */
public abstract class DynamicTableRecordGenerator implements DynamicRecordGenerator<RowData> {

  private final RowType rowType;
  private final Map<String, String> writeProps;

  public DynamicTableRecordGenerator(RowType rowType, Map<String, String> writeProps) {
    this.rowType = rowType;
    this.writeProps = writeProps;
  }

  public RowType rowType() {
    return rowType;
  }

  public Map<String, String> writeProps() {
    return writeProps;
  }

  protected Map<String, Integer> getFieldPositionIndex() {
    Map<String, Integer> fieldNameToPosition = Maps.newHashMap();
    List<RowType.RowField> fields = rowType.getFields();

    for (int i = 0; i < fields.size(); i++) {
      RowType.RowField field = fields.get(i);
      fieldNameToPosition.put(field.getName(), i);
    }

    return fieldNameToPosition;
  }

  protected void validateRequiredFieldAndType(String columnName, LogicalType expectedType) {
    int fieldIndex = rowType.getFieldIndex(columnName);

    Preconditions.checkArgument(fieldIndex != -1, "Missing column %s", columnName);

    LogicalType actualType = rowType.getTypeAt(fieldIndex);
    Preconditions.checkArgument(
        actualType.is(expectedType.getTypeRoot()),
        "Invalid column type for %s:%s. Expected column type:%s",
        columnName,
        actualType,
        expectedType);
  }
}
