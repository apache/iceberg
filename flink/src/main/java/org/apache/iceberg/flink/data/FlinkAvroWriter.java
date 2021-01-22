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

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.avro.ValueWriter;
import org.apache.iceberg.avro.ValueWriters;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public class FlinkAvroWriter implements DatumWriter<RowData> {
  private final RowType rowType;
  private ValueWriter<RowData> writer = null;

  public FlinkAvroWriter(RowType rowType) {
    this.rowType = rowType;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void setSchema(Schema schema) {
    this.writer = (ValueWriter<RowData>) AvroWithFlinkSchemaVisitor
        .visit(rowType, schema, new WriteBuilder());
  }

  @Override
  public void write(RowData datum, Encoder out) throws IOException {
    writer.write(datum, out);
  }

  private static class WriteBuilder extends AvroWithFlinkSchemaVisitor<ValueWriter<?>> {
    @Override
    public ValueWriter<?> record(LogicalType struct, Schema record, List<String> names, List<ValueWriter<?>> fields) {
      return FlinkValueWriters.row(fields, IntStream.range(0, names.size())
          .mapToObj(i -> fieldNameAndType(struct, i).second()).collect(Collectors.toList()));
    }

    @Override
    public ValueWriter<?> union(LogicalType type, Schema union, List<ValueWriter<?>> options) {
      Preconditions.checkArgument(options.contains(ValueWriters.nulls()),
          "Cannot create writer for non-option union: %s", union);
      Preconditions.checkArgument(options.size() == 2,
          "Cannot create writer for non-option union: %s", union);
      if (union.getTypes().get(0).getType() == Schema.Type.NULL) {
        return ValueWriters.option(0, options.get(1));
      } else {
        return ValueWriters.option(1, options.get(0));
      }
    }

    @Override
    public ValueWriter<?> array(LogicalType sArray, Schema array, ValueWriter<?> elementWriter) {
      return FlinkValueWriters.array(elementWriter, arrayElementType(sArray));
    }

    @Override
    public ValueWriter<?> map(LogicalType sMap, Schema map, ValueWriter<?> valueReader) {
      return FlinkValueWriters.map(FlinkValueWriters.strings(), mapKeyType(sMap), valueReader, mapValueType(sMap));
    }

    @Override
    public ValueWriter<?> map(LogicalType sMap, Schema map, ValueWriter<?> keyWriter, ValueWriter<?> valueWriter) {
      return FlinkValueWriters.arrayMap(keyWriter, mapKeyType(sMap), valueWriter, mapValueType(sMap));
    }

    @Override
    public ValueWriter<?> primitive(LogicalType type, Schema primitive) {
      org.apache.avro.LogicalType logicalType = primitive.getLogicalType();
      if (logicalType != null) {
        switch (logicalType.getName()) {
          case "date":
            return ValueWriters.ints();

          case "time-micros":
            return FlinkValueWriters.timeMicros();

          case "timestamp-micros":
            return FlinkValueWriters.timestampMicros();

          case "decimal":
            LogicalTypes.Decimal decimal = (LogicalTypes.Decimal) logicalType;
            return FlinkValueWriters.decimal(decimal.getPrecision(), decimal.getScale());

          case "uuid":
            return ValueWriters.uuids();

          default:
            throw new IllegalArgumentException("Unsupported logical type: " + logicalType);
        }
      }

      switch (primitive.getType()) {
        case NULL:
          return ValueWriters.nulls();
        case BOOLEAN:
          return ValueWriters.booleans();
        case INT:
          switch (type.getTypeRoot()) {
            case TINYINT:
              return ValueWriters.tinyints();
            case SMALLINT:
              return ValueWriters.shorts();
            default:
              return ValueWriters.ints();
          }
        case LONG:
          return ValueWriters.longs();
        case FLOAT:
          return ValueWriters.floats();
        case DOUBLE:
          return ValueWriters.doubles();
        case STRING:
          return FlinkValueWriters.strings();
        case FIXED:
          return ValueWriters.fixed(primitive.getFixedSize());
        case BYTES:
          return ValueWriters.bytes();
        default:
          throw new IllegalArgumentException("Unsupported type: " + primitive);
      }
    }
  }
}
