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
package org.apache.iceberg.flink;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.NullType;

public class FlinkRowData {

  private FlinkRowData() {}

  public static RowData.FieldGetter createFieldGetter(LogicalType fieldType, int fieldPos) {
    if (fieldType instanceof NullType) {
      return rowData -> null;
    }

    RowData.FieldGetter flinkFieldGetter = RowData.createFieldGetter(fieldType, fieldPos);
    return rowData -> {
      // Be sure to check for null values, even if the field is required. Flink
      // RowData.createFieldGetter(..) does not null-check optional / nullable types. Without this
      // explicit null check, the null flag of BinaryRowData will be ignored and random bytes will
      // be parsed as actual values. This will produce incorrect writes instead of failing with a
      // NullPointerException. See https://issues.apache.org/jira/browse/FLINK-37245
      if (!fieldType.isNullable() && rowData.isNullAt(fieldPos)) {
        return null;
      }
      return flinkFieldGetter.getFieldOrNull(rowData);
    };
  }
}
