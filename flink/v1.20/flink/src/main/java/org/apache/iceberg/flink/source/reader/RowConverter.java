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
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.jetbrains.annotations.NotNull;

public class RowConverter implements RowDataConverter<Row> {
    private final DataStructureConverter<Object, Object> converter;
    private final TypeInformation<Row> outputTypeInfo;

    private RowConverter(RowType rowType) {
        this.converter = DataStructureConverters.getConverter(TypeConversions.fromLogicalToDataType(rowType));
        this.outputTypeInfo = getTypeInfo(rowType);
    }

    public static RowConverter fromIcebergSchema(
            org.apache.iceberg.Schema icebergSchema) {
        RowType rowType = FlinkSchemaUtil.convert(icebergSchema);
        return new RowConverter(rowType);
    }

    private static @NotNull RowTypeInfo getTypeInfo(RowType rowType) {
        int fieldCount = rowType.getFieldCount();
        TypeInformation<?>[] fieldTypes = new TypeInformation<?>[fieldCount];
        String[] fieldNames = new String[fieldCount];

        for (int i = 0; i < fieldCount; i++) {
            LogicalType logicalType = rowType.getTypeAt(i);
            fieldTypes[i] = TypeInformation.of(logicalType.getDefaultConversion());
            fieldNames[i] = rowType.getFieldNames().get(i);
        }

        return new RowTypeInfo(fieldTypes, fieldNames);
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
