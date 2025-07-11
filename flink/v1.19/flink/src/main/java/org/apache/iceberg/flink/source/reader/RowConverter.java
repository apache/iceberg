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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.apache.iceberg.flink.FlinkSchemaUtil;

public class RowConverter implements RowDataConverter<Row> {
  private final DataStructureConverter<Object, Object> converter;
  private final TypeInformation<Row> outputTypeInfo;

  private RowConverter(RowType rowType, TypeInformation<Row> rowTypeInfo) {
    this.converter =
        DataStructureConverters.getConverter(TypeConversions.fromLogicalToDataType(rowType));
    this.outputTypeInfo = rowTypeInfo;
  }

  public static RowConverter fromIcebergSchema(org.apache.iceberg.Schema icebergSchema) {
    RowType rowType = FlinkSchemaUtil.convert(icebergSchema);
    ResolvedSchema resolvedSchema = FlinkSchemaUtil.toResolvedSchema(icebergSchema);
    TypeInformation<?>[] types =
        resolvedSchema.getColumnDataTypes().stream()
            .map(ExternalTypeInfo::of)
            .toArray(TypeInformation[]::new);
    String[] fieldNames = resolvedSchema.getColumnNames().toArray(String[]::new);
    RowTypeInfo rowTypeInfo = new RowTypeInfo(types, fieldNames);
    return new RowConverter(rowType, rowTypeInfo);
  }

  @Override
  public Row apply(RowData rowData) {
    return (Row) converter.toExternal(rowData);
  }

  @Override
  public TypeInformation<Row> getProducedType() {
    return outputTypeInfo;
  }
}
