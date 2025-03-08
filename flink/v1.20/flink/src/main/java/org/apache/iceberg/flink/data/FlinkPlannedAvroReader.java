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
import java.util.Map;
import java.util.function.Supplier;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.avro.AvroWithPartnerVisitor;
import org.apache.iceberg.avro.SupportsRowPosition;
import org.apache.iceberg.avro.ValueReader;
import org.apache.iceberg.avro.ValueReaders;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;

public class FlinkPlannedAvroReader implements DatumReader<RowData>, SupportsRowPosition {

  private final Types.StructType expectedType;
  private final Map<Integer, ?> idToConstant;
  private ValueReader<RowData> reader;

  public static FlinkPlannedAvroReader create(org.apache.iceberg.Schema schema) {
    return create(schema, ImmutableMap.of());
  }

  public static FlinkPlannedAvroReader create(
      org.apache.iceberg.Schema schema, Map<Integer, ?> constants) {
    return new FlinkPlannedAvroReader(schema, constants);
  }

  private FlinkPlannedAvroReader(
      org.apache.iceberg.Schema expectedSchema, Map<Integer, ?> constants) {
    this.expectedType = expectedSchema.asStruct();
    this.idToConstant = constants;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void setSchema(Schema fileSchema) {
    this.reader =
        (ValueReader<RowData>)
            AvroWithPartnerVisitor.visit(
                expectedType,
                fileSchema,
                new ReadBuilder(idToConstant),
                AvroWithPartnerVisitor.FieldIDAccessors.get());
  }

  @Override
  public RowData read(RowData reuse, Decoder decoder) throws IOException {
    return reader.read(decoder, reuse);
  }

  @Override
  public void setRowPositionSupplier(Supplier<Long> posSupplier) {
    if (reader instanceof SupportsRowPosition) {
      ((SupportsRowPosition) reader).setRowPositionSupplier(posSupplier);
    }
  }

  private static class ReadBuilder extends AvroWithPartnerVisitor<Type, ValueReader<?>> {
    private final Map<Integer, ?> idToConstant;

    private ReadBuilder(Map<Integer, ?> idToConstant) {
      this.idToConstant = idToConstant;
    }

    @Override
    public ValueReader<?> record(Type partner, Schema record, List<ValueReader<?>> fieldReaders) {
      if (partner == null) {
        return ValueReaders.skipStruct(fieldReaders);
      }

      Types.StructType expected = partner.asStructType();
      List<Pair<Integer, ValueReader<?>>> readPlan =
          ValueReaders.buildReadPlan(
              expected, record, fieldReaders, idToConstant, RowDataUtil::convertConstant);

      // TODO: should this pass expected so that struct.get can reuse containers?
      return FlinkValueReaders.struct(readPlan, expected.fields().size());
    }

    @Override
    public ValueReader<?> union(Type partner, Schema union, List<ValueReader<?>> options) {
      return ValueReaders.union(options);
    }

    @Override
    public ValueReader<?> array(Type partner, Schema array, ValueReader<?> elementReader) {
      return FlinkValueReaders.array(elementReader);
    }

    @Override
    public ValueReader<?> arrayMap(
        Type partner, Schema map, ValueReader<?> keyReader, ValueReader<?> valueReader) {
      return FlinkValueReaders.arrayMap(keyReader, valueReader);
    }

    @Override
    public ValueReader<?> map(Type partner, Schema map, ValueReader<?> valueReader) {
      return FlinkValueReaders.map(FlinkValueReaders.strings(), valueReader);
    }

    @Override
    public ValueReader<?> primitive(Type partner, Schema primitive) {
      LogicalType logicalType = primitive.getLogicalType();
      if (logicalType != null) {
        switch (logicalType.getName()) {
          case "date":
            // Flink uses the same representation
            return ValueReaders.ints();

          case "time-micros":
            return FlinkValueReaders.timeMicros();

          case "timestamp-millis":
            return FlinkValueReaders.timestampMills();

          case "timestamp-micros":
            return FlinkValueReaders.timestampMicros();

          case "timestamp-nanos":
            return FlinkValueReaders.timestampNanos();

          case "decimal":
            LogicalTypes.Decimal decimal = (LogicalTypes.Decimal) logicalType;
            return FlinkValueReaders.decimal(
                ValueReaders.decimalBytesReader(primitive),
                decimal.getPrecision(),
                decimal.getScale());

          case "uuid":
            return FlinkValueReaders.uuids();

          default:
            throw new IllegalArgumentException("Unknown logical type: " + logicalType.getName());
        }
      }

      switch (primitive.getType()) {
        case NULL:
          return ValueReaders.nulls();
        case BOOLEAN:
          return ValueReaders.booleans();
        case INT:
          if (partner != null && partner.typeId() == Type.TypeID.LONG) {
            return ValueReaders.intsAsLongs();
          }
          return ValueReaders.ints();
        case LONG:
          return ValueReaders.longs();
        case FLOAT:
          if (partner != null && partner.typeId() == Type.TypeID.DOUBLE) {
            return ValueReaders.floatsAsDoubles();
          }
          return ValueReaders.floats();
        case DOUBLE:
          return ValueReaders.doubles();
        case STRING:
          return FlinkValueReaders.strings();
        case FIXED:
          return ValueReaders.fixed(primitive.getFixedSize());
        case BYTES:
          return ValueReaders.bytes();
        case ENUM:
          return FlinkValueReaders.enums(primitive.getEnumSymbols());
        default:
          throw new IllegalArgumentException("Unsupported type: " + primitive);
      }
    }
  }
}
