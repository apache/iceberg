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

import java.util.Deque;
import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.FieldMetrics;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.orc.GenericOrcWriters;
import org.apache.iceberg.orc.OrcRowWriter;
import org.apache.iceberg.orc.OrcValueWriter;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;

public class FlinkOrcWriter implements OrcRowWriter<RowData> {
  private final FlinkOrcWriters.RowDataWriter writer;

  private FlinkOrcWriter(RowType rowType, Schema iSchema) {
    this.writer =
        (FlinkOrcWriters.RowDataWriter)
            FlinkSchemaVisitor.visit(rowType, iSchema, new WriteBuilder());
  }

  public static OrcRowWriter<RowData> buildWriter(RowType rowType, Schema iSchema) {
    return new FlinkOrcWriter(rowType, iSchema);
  }

  @Override
  public void write(RowData row, VectorizedRowBatch output) {
    Preconditions.checkArgument(row != null, "value must not be null");
    writer.writeRow(row, output);
  }

  @Override
  public List<OrcValueWriter<?>> writers() {
    return writer.writers();
  }

  @Override
  public Stream<FieldMetrics<?>> metrics() {
    return writer.metrics();
  }

  private static class WriteBuilder extends FlinkSchemaVisitor<OrcValueWriter<?>> {
    private final Deque<Integer> fieldIds = Lists.newLinkedList();

    private WriteBuilder() {}

    @Override
    public void beforeField(Types.NestedField field) {
      fieldIds.push(field.fieldId());
    }

    @Override
    public void afterField(Types.NestedField field) {
      fieldIds.pop();
    }

    @Override
    public OrcValueWriter<RowData> record(
        Types.StructType iStruct, List<OrcValueWriter<?>> results, List<LogicalType> fieldType) {
      return FlinkOrcWriters.struct(results, fieldType);
    }

    @Override
    public OrcValueWriter<?> map(
        Types.MapType iMap,
        OrcValueWriter<?> key,
        OrcValueWriter<?> value,
        LogicalType keyType,
        LogicalType valueType) {
      return FlinkOrcWriters.map(key, value, keyType, valueType);
    }

    @Override
    public OrcValueWriter<?> list(
        Types.ListType iList, OrcValueWriter<?> element, LogicalType elementType) {
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
          Preconditions.checkArgument(
              fieldIds.peek() != null,
              String.format(
                  "[BUG] Cannot find field id for primitive field with type %s. This is likely because id "
                      + "information is not properly pushed during schema visiting.",
                  iPrimitive));
          return GenericOrcWriters.floats(fieldIds.peek());
        case DOUBLE:
          Preconditions.checkArgument(
              fieldIds.peek() != null,
              String.format(
                  "[BUG] Cannot find field id for primitive field with type %s. This is likely because id "
                      + "information is not properly pushed during schema visiting.",
                  iPrimitive));
          return GenericOrcWriters.doubles(fieldIds.peek());
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
          throw new IllegalArgumentException(
              String.format(
                  "Invalid iceberg type %s corresponding to Flink logical type %s",
                  iPrimitive, flinkPrimitive));
      }
    }
  }
}
