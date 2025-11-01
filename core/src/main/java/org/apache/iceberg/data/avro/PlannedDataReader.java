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
package org.apache.iceberg.data.avro;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.avro.AvroWithPartnerVisitor;
import org.apache.iceberg.avro.SupportsRowPosition;
import org.apache.iceberg.avro.ValueReader;
import org.apache.iceberg.avro.ValueReaders;
import org.apache.iceberg.data.GenericDataUtil;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;

public class PlannedDataReader<T> implements DatumReader<T>, SupportsRowPosition {

  public static <D> PlannedDataReader<D> create(org.apache.iceberg.Schema expectedSchema) {
    return create(expectedSchema, ImmutableMap.of());
  }

  public static <D> PlannedDataReader<D> create(
      org.apache.iceberg.Schema expectedSchema, Map<Integer, ?> idToConstant) {
    return new PlannedDataReader<>(expectedSchema, idToConstant);
  }

  private final org.apache.iceberg.Schema expectedSchema;
  private final Map<Integer, ?> idToConstant;
  private ValueReader<T> reader;

  protected PlannedDataReader(
      org.apache.iceberg.Schema expectedSchema, Map<Integer, ?> idToConstant) {
    this.expectedSchema = expectedSchema;
    this.idToConstant = idToConstant;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void setSchema(Schema fileSchema) {
    this.reader =
        (ValueReader<T>)
            AvroWithPartnerVisitor.visit(
                expectedSchema.asStruct(),
                fileSchema,
                new ReadBuilder(idToConstant),
                AvroWithPartnerVisitor.FieldIDAccessors.get());
  }

  @Override
  public T read(T reuse, Decoder decoder) throws IOException {
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
              expected, record, fieldReaders, idToConstant, GenericDataUtil::internalToGeneric);

      return GenericReaders.struct(readPlan, expected);
    }

    @Override
    public ValueReader<?> union(Type partner, Schema union, List<ValueReader<?>> options) {
      return ValueReaders.union(options);
    }

    @Override
    public ValueReader<?> array(Type ignored, Schema array, ValueReader<?> elementReader) {
      return ValueReaders.array(elementReader);
    }

    @Override
    public ValueReader<?> arrayMap(
        Type ignored, Schema map, ValueReader<?> keyReader, ValueReader<?> valueReader) {
      return ValueReaders.arrayMap(keyReader, valueReader);
    }

    @Override
    public ValueReader<?> map(Type ignored, Schema map, ValueReader<?> valueReader) {
      return ValueReaders.map(ValueReaders.strings(), valueReader);
    }

    @Override
    public ValueReader<?> variant(
        Type partner, ValueReader<?> metadataReader, ValueReader<?> valueReader) {
      return ValueReaders.variants();
    }

    @Override
    public ValueReader<?> primitive(Type partner, Schema primitive) {
      LogicalType logicalType = primitive.getLogicalType();
      if (logicalType != null) {
        switch (logicalType.getName()) {
          case "date":
            return GenericReaders.dates();

          case "time-micros":
            return GenericReaders.times();

          case "timestamp-micros":
            if (AvroSchemaUtil.isTimestamptz(primitive)) {
              return GenericReaders.timestamptz();
            }
            return GenericReaders.timestamps();

          case "timestamp-nanos":
            if (AvroSchemaUtil.isTimestamptz(primitive)) {
              return GenericReaders.timestamptzNanos();
            }
            return GenericReaders.timestampNanos();

          case "timestamp-millis":
            if (AvroSchemaUtil.isTimestamptz(primitive)) {
              return GenericReaders.timestamptzMillis();
            }
            return GenericReaders.timestampMillis();

          case "decimal":
            return ValueReaders.decimal(
                ValueReaders.decimalBytesReader(primitive),
                ((LogicalTypes.Decimal) logicalType).getScale());

          case "uuid":
            return ValueReaders.uuids();

          default:
            throw new IllegalArgumentException("Unknown logical type: " + logicalType);
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
          // might want to use a binary-backed container like Utf8
          return ValueReaders.strings();
        case FIXED:
          return ValueReaders.fixed(primitive.getFixedSize());
        case BYTES:
          return ValueReaders.byteBuffers();
        default:
          throw new IllegalArgumentException("Unsupported type: " + primitive);
      }
    }
  }
}
