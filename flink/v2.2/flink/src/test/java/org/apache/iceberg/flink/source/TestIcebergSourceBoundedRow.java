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
package org.apache.iceberg.flink.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.types.Row;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.source.reader.RowConverter;
import org.apache.iceberg.flink.source.reader.RowDataConverter;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestIcebergSourceBoundedRow extends TestIcebergSourceBoundedConverterBase<Row> {

  @Override
  protected RowDataConverter<Row> getConverter(Schema icebergSchema, Table table) {
    return RowConverter.fromIcebergSchema(icebergSchema);
  }

  @Override
  protected TypeInformation<Row> getTypeInfo(Schema icebergSchema) {
    ResolvedSchema resolvedSchema = FlinkSchemaUtil.toResolvedSchema(icebergSchema);
    TypeInformation<?>[] types =
        resolvedSchema.getColumnDataTypes().stream()
            .map(ExternalTypeInfo::of)
            .toArray(TypeInformation[]::new);
    String[] fieldNames = resolvedSchema.getColumnNames().toArray(String[]::new);
    return new RowTypeInfo(types, fieldNames);
  }

  @Override
  protected DataStream<Row> mapToRow(DataStream<Row> inputStream, Schema icebergSchema) {
    return inputStream;
  }
}
