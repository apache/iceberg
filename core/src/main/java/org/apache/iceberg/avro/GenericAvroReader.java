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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.ResolvingDecoder;
import org.apache.iceberg.common.DynClasses;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.relocated.com.google.common.collect.MapMaker;

class GenericAvroReader<T> implements DatumReader<T> {

  private static final ThreadLocal<Map<Schema, Map<Schema, ResolvingDecoder>>> DECODER_CACHES =
      ThreadLocal.withInitial(() -> new MapMaker().weakKeys().makeMap());

  private final Schema readSchema;
  private ClassLoader loader = Thread.currentThread().getContextClassLoader();
  private Schema fileSchema = null;
  private ValueReader<T> reader = null;

  GenericAvroReader(Schema readSchema) {
    this.readSchema = readSchema;
  }

  @SuppressWarnings("unchecked")
  private void initReader() {
    this.reader = (ValueReader<T>) AvroSchemaVisitor.visit(readSchema, new ReadBuilder(loader));
  }

  @Override
  public void setSchema(Schema schema) {
    this.fileSchema = Schema.applyAliases(schema, readSchema);
    initReader();
  }

  public void setClassLoader(ClassLoader newClassLoader) {
    this.loader = newClassLoader;
  }

  @Override
  public T read(T reuse, Decoder decoder) throws IOException {
    ResolvingDecoder resolver = resolve(decoder);
    T value = reader.read(resolver, reuse);
    resolver.drain();
    return value;
  }

  private ResolvingDecoder resolve(Decoder decoder) throws IOException {
    Map<Schema, Map<Schema, ResolvingDecoder>> cache = DECODER_CACHES.get();
    Map<Schema, ResolvingDecoder> fileSchemaToResolver = cache
        .computeIfAbsent(readSchema, k -> new HashMap<>());

    ResolvingDecoder resolver = fileSchemaToResolver.get(fileSchema);
    if (resolver == null) {
      resolver = newResolver();
      fileSchemaToResolver.put(fileSchema, resolver);
    }

    resolver.configure(decoder);

    return resolver;
  }

  private ResolvingDecoder newResolver() {
    try {
      return DecoderFactory.get().resolvingDecoder(fileSchema, readSchema, null);
    } catch (IOException e) {
      throw new RuntimeIOException(e);
    }
  }

  private static class ReadBuilder extends AvroSchemaVisitor<ValueReader<?>> {
    private final ClassLoader loader;

    private ReadBuilder(ClassLoader loader) {
      this.loader = loader;
    }

    @Override
    @SuppressWarnings("unchecked")
    public ValueReader<?> record(Schema record, List<String> names, List<ValueReader<?>> fields) {
      try {
        Class<?> recordClass = DynClasses.builder()
            .loader(loader)
            .impl(record.getFullName())
            .buildChecked();
        if (IndexedRecord.class.isAssignableFrom(recordClass)) {
          return ValueReaders.record(fields, (Class<? extends IndexedRecord>) recordClass, record);
        }

        return ValueReaders.record(fields, record);

      } catch (ClassNotFoundException e) {
        return ValueReaders.record(fields, record);
      }
    }

    @Override
    public ValueReader<?> union(Schema union, List<ValueReader<?>> options) {
      return ValueReaders.union(options);
    }

    @Override
    public ValueReader<?> array(Schema array, ValueReader<?> elementReader) {
      if (array.getLogicalType() instanceof LogicalMap) {
        ValueReaders.StructReader<?> keyValueReader = (ValueReaders.StructReader) elementReader;
        ValueReader<?> keyReader = keyValueReader.reader(0);
        ValueReader<?> valueReader = keyValueReader.reader(1);

        if (keyReader == ValueReaders.utf8s()) {
          return ValueReaders.arrayMap(ValueReaders.strings(), valueReader);
        }

        return ValueReaders.arrayMap(keyReader, valueReader);
      }

      return ValueReaders.array(elementReader);
    }

    @Override
    public ValueReader<?> map(Schema map, ValueReader<?> valueReader) {
      return ValueReaders.map(ValueReaders.strings(), valueReader);
    }

    @Override
    public ValueReader<?> primitive(Schema primitive) {
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
            ValueReader<byte[]> inner;
            switch (primitive.getType()) {
              case FIXED:
                inner = ValueReaders.fixed(primitive.getFixedSize());
                break;
              case BYTES:
                inner = ValueReaders.bytes();
                break;
              default:
                throw new IllegalArgumentException(
                    "Invalid primitive type for decimal: " + primitive.getType());
            }

            LogicalTypes.Decimal decimal = (LogicalTypes.Decimal) logicalType;
            return ValueReaders.decimal(inner, decimal.getScale());

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
          return ValueReaders.ints();
        case LONG:
          return ValueReaders.longs();
        case FLOAT:
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
