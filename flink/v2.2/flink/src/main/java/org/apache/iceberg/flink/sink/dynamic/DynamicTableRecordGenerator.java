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
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/**
 * Abstract base class for SQL-based dynamic record generators. Users will extend this class to
 * create a DynamicRecord from RowData.
 */
public abstract class DynamicTableRecordGenerator implements DynamicRecordGenerator<RowData> {

  private final RowType rowType;
  private final Configuration flinkConfiguration;
  private final Map<String, String> writeProperties;
  private final Map<String, Integer> fieldNameToPosition;
  private transient FlinkDynamicSinkConf flinkDynamicSinkConf;

  public DynamicTableRecordGenerator(RowType rowType) {
    this(rowType, ImmutableMap.of(), new Configuration());
  }

  public DynamicTableRecordGenerator(
      RowType rowType, Map<String, String> writeProperties, Configuration flinkConfiguration) {
    this.rowType = rowType;
    this.writeProperties = writeProperties;
    this.flinkConfiguration = flinkConfiguration;
    this.fieldNameToPosition = fieldNameToPositionMapping();
  }

  @Override
  public void open(OpenContext openContext) throws Exception {
    this.flinkDynamicSinkConf = new FlinkDynamicSinkConf(writeProperties, flinkConfiguration);
  }

  protected RowType rowType() {
    return rowType;
  }

  protected FlinkDynamicSinkConf flinkDynamicSinkConf() {
    return flinkDynamicSinkConf;
  }

  protected Map<String, Integer> fieldNameToPosition() {
    return fieldNameToPosition;
  }

  protected void validateRequiredColumnAndType(String columnName, LogicalType expectedType) {
    int fieldIndex = rowType.getFieldIndex(columnName);
    Preconditions.checkArgument(
        fieldIndex != -1,
        "Missing column %s. Expected column %s of type %s.",
        columnName,
        columnName,
        expectedType);

    LogicalType actualType = rowType.getTypeAt(fieldIndex);
    Preconditions.checkArgument(
        actualType.is(expectedType.getTypeRoot()),
        "Invalid column type for %s: %s. Expected column type: %s",
        columnName,
        actualType,
        expectedType);
  }

  private Map<String, Integer> fieldNameToPositionMapping() {
    Map<String, Integer> fieldNameToPositionMap = Maps.newHashMap();
    List<RowType.RowField> fields = this.rowType.getFields();

    for (int i = 0; i < fields.size(); i++) {
      RowType.RowField field = fields.get(i);
      fieldNameToPositionMap.put(field.getName(), i);
    }

    return fieldNameToPositionMap;
  }
}
