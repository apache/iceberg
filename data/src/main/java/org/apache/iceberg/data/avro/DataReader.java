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

import com.google.common.collect.MapMaker;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.ResolvingDecoder;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.avro.AvroSchemaWithTypeVisitor;
import org.apache.iceberg.avro.ValueReader;
import org.apache.iceberg.avro.ValueReaders;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

public class DataReader<T> implements DatumReader<T> {

  private static final ThreadLocal<Map<Schema, Map<Schema, ResolvingDecoder>>> DECODER_CACHES =
      ThreadLocal.withInitial(() -> new MapMaker().weakKeys().makeMap());

  public static <D> DataReader<D> create(org.apache.iceberg.Schema expectedSchema, Schema readSchema) {
    return new DataReader<>(expectedSchema, readSchema);
  }

  private final Schema readSchema;
  private final ValueReader<T> reader;
  private Schema fileSchema = null;

  @SuppressWarnings("unchecked")
  private DataReader(org.apache.iceberg.Schema expectedSchema, Schema readSchema) {
    this.readSchema = readSchema;
    this.reader = (ValueReader<T>) AvroSchemaWithTypeVisitor.visit(expectedSchema, readSchema, new ReadBuilder());
  }

  @Override
  public void setSchema(Schema newFileSchema) {
    this.fileSchema = Schema.applyAliases(newFileSchema, readSchema);
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

  private static class ReadBuilder extends AvroSchemaWithTypeVisitor<ValueReader<?>> {

    private ReadBuilder() {
    }

    @Override
    public ValueReader<?> record(Types.StructType struct, Schema record,
                                 List<String> names, List<ValueReader<?>> fields) {
      return GenericReaders.struct(struct, fields);
    }

    @Override
    public ValueReader<?> union(Type ignored, Schema union, List<ValueReader<?>> options) {
      return ValueReaders.union(options);
    }

    @Override
    public ValueReader<?> array(Types.ListType ignored, Schema array, ValueReader<?> elementReader) {
      return ValueReaders.array(elementReader);
    }

    @Override
    public ValueReader<?> map(Types.MapType iMap, Schema map, ValueReader<?> keyReader, ValueReader<?> valueReader) {
      return ValueReaders.arrayMap(keyReader, valueReader);
    }

    @Override
    public ValueReader<?> map(Types.MapType ignored, Schema map, ValueReader<?> valueReader) {
      return ValueReaders.map(ValueReaders.strings(), valueReader);
    }

    @Override
    public ValueReader<?> primitive(Type.PrimitiveType ignored, Schema primitive) {
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
