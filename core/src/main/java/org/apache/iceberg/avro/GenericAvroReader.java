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
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.iceberg.common.DynClasses;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;

public class GenericAvroReader<T>
    implements DatumReader<T>, SupportsRowPosition, SupportsCustomRecords {

  private final Types.StructType expectedType;
  private ClassLoader loader = Thread.currentThread().getContextClassLoader();
  private Map<String, String> renames = ImmutableMap.of();
  private final Map<Integer, Object> idToConstant = ImmutableMap.of();
  private Schema fileSchema = null;
  private ValueReader<T> reader = null;

  public static <D> GenericAvroReader<D> create(org.apache.iceberg.Schema expectedSchema) {
    return new GenericAvroReader<>(expectedSchema);
  }

  public static <D> GenericAvroReader<D> create(Schema readSchema) {
    return new GenericAvroReader<>(readSchema);
  }

  GenericAvroReader(org.apache.iceberg.Schema expectedSchema) {
    this.expectedType = expectedSchema.asStruct();
  }

  GenericAvroReader(Schema readSchema) {
    this.expectedType = AvroSchemaUtil.convert(readSchema).asStructType();
  }

  @SuppressWarnings("unchecked")
  private void initReader() {
    this.reader =
        (ValueReader<T>)
            AvroWithPartnerVisitor.visit(
                expectedType,
                fileSchema,
                new ResolvingReadBuilder(expectedType, fileSchema.getFullName()),
                AvroWithPartnerVisitor.FieldIDAccessors.get());
  }

  @Override
  public void setSchema(Schema schema) {
    this.fileSchema = schema;
    initReader();
  }

  @Override
  public void setClassLoader(ClassLoader newClassLoader) {
    this.loader = newClassLoader;
  }

  @Override
  public void setRenames(Map<String, String> renames) {
    this.renames = renames;
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

  private class ResolvingReadBuilder extends AvroWithPartnerVisitor<Type, ValueReader<?>> {
    private final Map<Type, Schema> avroSchemas;

    private ResolvingReadBuilder(Types.StructType expectedType, String rootName) {
      this.avroSchemas = AvroSchemaUtil.convertTypes(expectedType, rootName);
    }

    @Override
    public ValueReader<?> record(Type partner, Schema record, List<ValueReader<?>> fieldResults) {
      if (partner == null) {
        return ValueReaders.skipStruct(fieldResults);
      }

      Types.StructType expected = partner.asStructType();
      List<Pair<Integer, ValueReader<?>>> readPlan =
          ValueReaders.buildReadPlan(expected, record, fieldResults, idToConstant);

      return recordReader(readPlan, avroSchemas.get(partner), record.getFullName());
    }

    @SuppressWarnings("unchecked")
    private ValueReader<?> recordReader(
        List<Pair<Integer, ValueReader<?>>> readPlan, Schema avroSchema, String recordName) {
      String className = renames.getOrDefault(recordName, recordName);
      if (className != null) {
        try {
          Class<?> recordClass = DynClasses.builder().loader(loader).impl(className).buildChecked();
          if (IndexedRecord.class.isAssignableFrom(recordClass)) {
            return ValueReaders.record(
                avroSchema, (Class<? extends IndexedRecord>) recordClass, readPlan);
          }
        } catch (ClassNotFoundException e) {
          // use a generic record reader below
        }
      }

      return ValueReaders.record(avroSchema, readPlan);
    }

    @Override
    public ValueReader<?> union(Type partner, Schema union, List<ValueReader<?>> options) {
      return ValueReaders.union(options);
    }

    @Override
    public ValueReader<?> arrayMap(
        Type partner, Schema map, ValueReader<?> keyReader, ValueReader<?> valueReader) {
      if (keyReader == ValueReaders.utf8s()) {
        return ValueReaders.arrayMap(ValueReaders.strings(), valueReader);
      }

      return ValueReaders.arrayMap(keyReader, valueReader);
    }

    @Override
    public ValueReader<?> array(Type partner, Schema array, ValueReader<?> elementReader) {
      return ValueReaders.array(elementReader);
    }

    @Override
    public ValueReader<?> map(Type partner, Schema map, ValueReader<?> valueReader) {
      return ValueReaders.map(ValueReaders.strings(), valueReader);
    }

    @Override
    public ValueReader<?> primitive(Type partner, Schema primitive) {
      LogicalType logicalType = primitive.getLogicalType();
      if (logicalType != null) {
        switch (logicalType.getName()) {
          case "date":
            // Spark uses the same representation
            return ValueReaders.ints();

          case "time-micros":
            return ValueReaders.longs();

          case "timestamp-millis":
            // adjust to microseconds
            ValueReader<Long> longs = ValueReaders.longs();
            return (ValueReader<Long>) (decoder, ignored) -> longs.read(decoder, null) * 1000L;

          case "timestamp-micros":
            // Spark uses the same representation
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
          return ValueReaders.utf8s();
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
}
