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
package org.apache.iceberg.avro;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;

/**
 * A reader that produces Iceberg's internal in-memory object model.
 *
 * <p>Iceberg's internal in-memory object model produces the types defined in {@link
 * Type.TypeID#javaClass()}.
 *
 * @param <T> Java type returned by the reader
 */
public class InternalReader<T> implements DatumReader<T>, SupportsRowPosition {

  private final Types.StructType expectedType;
  private final Map<Integer, Class<? extends StructLike>> typeMap = Maps.newHashMap();
  private final Map<Integer, Object> idToConstant = ImmutableMap.of();
  private Schema fileSchema = null;
  private ValueReader<T> reader = null;

  public static <D> InternalReader<D> create(org.apache.iceberg.Schema schema) {
    return new InternalReader<>(schema);
  }

  InternalReader(org.apache.iceberg.Schema readSchema) {
    this.expectedType = readSchema.asStruct();
  }

  @SuppressWarnings("unchecked")
  private void initReader() {
    this.reader =
        (ValueReader<T>)
            AvroWithPartnerVisitor.visit(
                Pair.of(-1, expectedType),
                fileSchema,
                new ResolvingReadBuilder(),
                AccessByID.instance());
  }

  @Override
  public void setSchema(Schema schema) {
    this.fileSchema = schema;
    initReader();
  }

  public InternalReader<T> setRootType(Class<? extends StructLike> rootClass) {
    typeMap.put(-1, rootClass);
    return this;
  }

  public InternalReader<T> setCustomType(int fieldId, Class<? extends StructLike> structClass) {
    typeMap.put(fieldId, structClass);
    return this;
  }

  @Override
  public void setRowPositionSupplier(Supplier<Long> posSupplier) {
    if (reader instanceof SupportsRowPosition) {
      ((SupportsRowPosition) reader).setRowPositionSupplier(posSupplier);
    }
  }

  @Override
  public T read(T reuse, Decoder decoder) throws IOException {
    return reader.read(decoder, reuse);
  }

  private class ResolvingReadBuilder
      extends AvroWithPartnerVisitor<Pair<Integer, Type>, ValueReader<?>> {
    @Override
    public ValueReader<?> record(
        Pair<Integer, Type> partner, Schema record, List<ValueReader<?>> fieldResults) {
      if (partner == null) {
        return ValueReaders.skipStruct(fieldResults);
      }

      Types.StructType expected = partner.second().asStructType();
      List<Pair<Integer, ValueReader<?>>> readPlan =
          ValueReaders.buildReadPlan(expected, record, fieldResults, idToConstant);

      return structReader(readPlan, partner.first(), expected);
    }

    private ValueReader<?> structReader(
        List<Pair<Integer, ValueReader<?>>> readPlan, int fieldId, Types.StructType struct) {

      Class<? extends StructLike> structClass = typeMap.get(fieldId);
      if (structClass != null) {
        return InternalReaders.struct(struct, structClass, readPlan);
      } else {
        return InternalReaders.struct(struct, readPlan);
      }
    }

    @Override
    public ValueReader<?> union(
        Pair<Integer, Type> partner, Schema union, List<ValueReader<?>> options) {
      return ValueReaders.union(options);
    }

    @Override
    public ValueReader<?> arrayMap(
        Pair<Integer, Type> partner,
        Schema map,
        ValueReader<?> keyReader,
        ValueReader<?> valueReader) {
      return ValueReaders.arrayMap(keyReader, valueReader);
    }

    @Override
    public ValueReader<?> array(
        Pair<Integer, Type> partner, Schema array, ValueReader<?> elementReader) {
      return ValueReaders.array(elementReader);
    }

    @Override
    public ValueReader<?> map(Pair<Integer, Type> partner, Schema map, ValueReader<?> valueReader) {
      return ValueReaders.map(ValueReaders.strings(), valueReader);
    }

    @Override
    public ValueReader<?> primitive(Pair<Integer, Type> partner, Schema primitive) {
      LogicalType logicalType = primitive.getLogicalType();
      if (logicalType != null) {
        switch (logicalType.getName()) {
          case "date":
            return ValueReaders.ints();

          case "time-micros":
            return ValueReaders.longs();

          case "timestamp-millis":
            // adjust to microseconds
            ValueReader<Long> longs = ValueReaders.longs();
            return (ValueReader<Long>) (decoder, ignored) -> longs.read(decoder, null) * 1000L;

          case "timestamp-micros":
            return ValueReaders.longs();

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
          if (partner != null && partner.second().typeId() == Type.TypeID.LONG) {
            return ValueReaders.intsAsLongs();
          }
          return ValueReaders.ints();
        case LONG:
          return ValueReaders.longs();
        case FLOAT:
          if (partner != null && partner.second().typeId() == Type.TypeID.DOUBLE) {
            return ValueReaders.floatsAsDoubles();
          }
          return ValueReaders.floats();
        case DOUBLE:
          return ValueReaders.doubles();
        case STRING:
          return ValueReaders.strings();
        case FIXED:
          return ValueReaders.fixed(primitive);
        case BYTES:
          return ValueReaders.byteBuffers();
        case ENUM:
          return ValueReaders.enums(primitive.getEnumSymbols());
        default:
          throw new IllegalArgumentException("Unsupported type: " + primitive);
      }
    }
  }

  private static class AccessByID
      implements AvroWithPartnerVisitor.PartnerAccessors<Pair<Integer, Type>> {
    private static final AccessByID INSTANCE = new AccessByID();

    public static AccessByID instance() {
      return INSTANCE;
    }

    @Override
    public Pair<Integer, Type> fieldPartner(
        Pair<Integer, Type> partner, Integer fieldId, String name) {
      Types.NestedField field = partner.second().asStructType().field(fieldId);
      return field != null ? Pair.of(field.fieldId(), field.type()) : null;
    }

    @Override
    public Pair<Integer, Type> mapKeyPartner(Pair<Integer, Type> partner) {
      Types.MapType map = partner.second().asMapType();
      return Pair.of(map.keyId(), map.keyType());
    }

    @Override
    public Pair<Integer, Type> mapValuePartner(Pair<Integer, Type> partner) {
      Types.MapType map = partner.second().asMapType();
      return Pair.of(map.valueId(), map.valueType());
    }

    @Override
    public Pair<Integer, Type> listElementPartner(Pair<Integer, Type> partner) {
      Types.ListType list = partner.second().asListType();
      return Pair.of(list.elementId(), list.elementType());
    }
  }
}
