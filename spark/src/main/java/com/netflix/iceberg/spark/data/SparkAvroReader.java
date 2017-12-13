/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.iceberg.spark.data;

import com.google.common.collect.MapMaker;
import com.netflix.iceberg.avro.AvroSchemaVisitor;
import com.netflix.iceberg.exceptions.RuntimeIOException;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.ResolvingDecoder;
import org.apache.spark.sql.catalyst.InternalRow;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class SparkAvroReader implements DatumReader<InternalRow> {

  private static final ThreadLocal<Map<Schema, Map<Schema, ResolvingDecoder>>> DECODER_CACHES =
      ThreadLocal.withInitial(() -> new MapMaker().weakKeys().makeMap());

  private final Schema readSchema;
  private Schema fileSchema = null;
  private ValueReader<InternalRow> reader = null;

  @SuppressWarnings("unchecked")
  public SparkAvroReader(Schema readSchema) {
    this.readSchema = readSchema;
    this.reader = (ValueReader<InternalRow>) AvroSchemaVisitor.visit(readSchema, ReadBuilder.get());
  }

  @Override
  public void setSchema(Schema fileSchema) {
    this.fileSchema = Schema.applyAliases(fileSchema, readSchema);
  }

  @Override
  public InternalRow read(InternalRow reuse, Decoder decoder) throws IOException {
    ResolvingDecoder resolver = resolve(decoder);
    InternalRow row = reader.read(resolver);
    resolver.drain();
    return row;
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
    private static final ReadBuilder INSTANCE = new ReadBuilder();

    public static ReadBuilder get() {
      return INSTANCE;
    }

    private ReadBuilder() {
    }

    @Override
    public ValueReader<?> record(Schema record, List<String> names, List<ValueReader<?>> fields) {
      return ValueReaders.struct(fields);
    }

    @Override
    public ValueReader<?> union(Schema union, List<ValueReader<?>> options) {
      return ValueReaders.union(options);
    }

    @Override
    public ValueReader<?> array(Schema array, ValueReader<?> elementReader) {
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

          case "timestamp-millis":
            // adjust to microseconds
            return (ValueReader<Long>) decoder -> ValueReaders.longs().read(decoder) * 1000L;

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
          return ValueReaders.strings();
        case FIXED:
          return ValueReaders.fixed(primitive.getFixedSize());
        case BYTES:
          return ValueReaders.bytes();
        default:
          throw new IllegalArgumentException("Unsupported type: " + primitive);
      }
    }
  }
}
