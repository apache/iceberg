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
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.orc.GenericOrcWriters;
import org.apache.iceberg.orc.OrcRowWriter;
import org.apache.iceberg.orc.OrcValueWriter;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;

public class FlinkOrcWriter implements OrcRowWriter<RowData> {
  private final FlinkOrcWriters.StructWriter writer;
  private final List<RowData.FieldGetter> fieldGetters;

  private FlinkOrcWriter(RowType rowType, Schema iSchema) {
    this.writer = (FlinkOrcWriters.StructWriter) FlinkSchemaVisitor.visit(rowType, iSchema, new WriteBuilder());

    List<LogicalType> fieldTypes = rowType.getChildren();
    this.fieldGetters = Lists.newArrayListWithExpectedSize(fieldTypes.size());
    for (int i = 0; i < fieldTypes.size(); i++) {
      fieldGetters.add(RowData.createFieldGetter(fieldTypes.get(i), i));
    }
  }

  public static OrcRowWriter<RowData> buildWriter(RowType rowType, Schema iSchema) {
    return new FlinkOrcWriter(rowType, iSchema);
  }

  @Override
  @SuppressWarnings("unchecked")
  public void write(RowData row, VectorizedRowBatch output) {
    int rowId = output.size;
    output.size += 1;

    List<OrcValueWriter<?>> writers = writer.writers();
    for (int c = 0; c < writers.size(); ++c) {
      OrcValueWriter child = writers.get(c);
      child.write(rowId, fieldGetters.get(c).getFieldOrNull(row), output.cols[c]);
    }
  }

  private static class WriteBuilder extends FlinkSchemaVisitor<OrcValueWriter<?>> {
    private WriteBuilder() {
    }

    @Override
    public OrcValueWriter<RowData> record(Types.StructType iStruct,
                                          List<OrcValueWriter<?>> results,
                                          List<LogicalType> fieldType) {
      return FlinkOrcWriters.struct(results, fieldType);
    }

    @Override
    public OrcValueWriter<?> map(Types.MapType iMap, OrcValueWriter<?> key, OrcValueWriter<?> value,
                                 LogicalType keyType, LogicalType valueType) {
      return FlinkOrcWriters.map(key, value, keyType, valueType);
    }

    @Override
    public OrcValueWriter<?> list(Types.ListType iList, OrcValueWriter<?> element, LogicalType elementType) {
      return FlinkOrcWriters.list(element, elementType);
    }

    @Override
    public OrcValueWriter<?> primitive(Type.PrimitiveType iPrimitive, LogicalType flinkPrimitive) {
      switch (iPrimitive.typeId()) {
        case BOOLEAN:
          return GenericOrcWriters.booleans();
        case INTEGER:
          switch (flinkPrimitive.getTypeRoot()) {
            case TINYINT:
              return GenericOrcWriters.bytes();
            case SMALLINT:
              return GenericOrcWriters.shorts();
          }
          return GenericOrcWriters.ints();
        case LONG:
          return GenericOrcWriters.longs();
        case FLOAT:
          return GenericOrcWriters.floats();
        case DOUBLE:
          return GenericOrcWriters.doubles();
        case DATE:
          return FlinkOrcWriters.dates();
        case TIME:
          return FlinkOrcWriters.times();
        case TIMESTAMP:
          Types.TimestampType timestampType = (Types.TimestampType) iPrimitive;
          if (timestampType.shouldAdjustToUTC()) {
            return FlinkOrcWriters.timestampTzs();
          } else {
            return FlinkOrcWriters.timestamps();
          }
        case STRING:
          return FlinkOrcWriters.strings();
        case UUID:
        case FIXED:
        case BINARY:
          return GenericOrcWriters.byteArrays();
        case DECIMAL:
          Types.DecimalType decimalType = (Types.DecimalType) iPrimitive;
          return FlinkOrcWriters.decimals(decimalType.precision(), decimalType.scale());
        default:
          throw new IllegalArgumentException(String.format(
              "Invalid iceberg type %s corresponding to Flink logical type %s", iPrimitive, flinkPrimitive));
      }
    }
  }
}
