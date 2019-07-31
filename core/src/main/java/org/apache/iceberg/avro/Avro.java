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

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificData;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;

import static org.apache.iceberg.TableProperties.AVRO_COMPRESSION;
import static org.apache.iceberg.TableProperties.AVRO_COMPRESSION_DEFAULT;

public class Avro {
  private Avro() {
  }

  private enum CodecName {
    UNCOMPRESSED(CodecFactory.nullCodec()),
    SNAPPY(CodecFactory.snappyCodec()),
    GZIP(CodecFactory.deflateCodec(9)),
    LZ4(null),
    BROTLI(null),
    ZSTD(null);

    private CodecFactory avroCodec;

    CodecName(CodecFactory avroCodec) {
      this.avroCodec = avroCodec;
    }

    public CodecFactory get() {
      Preconditions.checkArgument(avroCodec != null, "Missing implementation for codec %s", this);
      return avroCodec;
    }
  }

  private static final GenericData DEFAULT_MODEL = new SpecificData();

  static {
    LogicalTypes.register(LogicalMap.NAME, schema -> LogicalMap.get());
    DEFAULT_MODEL.addLogicalTypeConversion(new Conversions.DecimalConversion());
    DEFAULT_MODEL.addLogicalTypeConversion(new UUIDConversion());
  }

  public static WriteBuilder write(OutputFile file) {
    return new WriteBuilder(file);
  }

  public static class WriteBuilder {
    private final OutputFile file;
    private org.apache.iceberg.Schema schema = null;
    private String name = "table";
    private Map<String, String> config = Maps.newHashMap();
    private Map<String, String> metadata = Maps.newLinkedHashMap();
    private Function<Schema, DatumWriter<?>> createWriterFunc = GenericAvroWriter::new;
    private boolean overwrite;

    private WriteBuilder(OutputFile file) {
      this.file = file;
    }

    public WriteBuilder forTable(Table table) {
      schema(table.schema());
      setAll(table.properties());
      return this;
    }

    public WriteBuilder schema(org.apache.iceberg.Schema newSchema) {
      this.schema = newSchema;
      return this;
    }

    public WriteBuilder named(String newName) {
      this.name = newName;
      return this;
    }

    public WriteBuilder createWriterFunc(Function<Schema, DatumWriter<?>> writerFunction) {
      this.createWriterFunc = writerFunction;
      return this;
    }

    public WriteBuilder set(String property, String value) {
      config.put(property, value);
      return this;
    }

    public WriteBuilder setAll(Map<String, String> properties) {
      config.putAll(properties);
      return this;
    }

    public WriteBuilder meta(String property, String value) {
      metadata.put(property, value);
      return this;
    }

    public WriteBuilder meta(Map<String, String> properties) {
      metadata.putAll(properties);
      return this;
    }

    public WriteBuilder overwrite() {
      return overwrite(true);
    }

    public WriteBuilder overwrite(boolean enabled) {
      this.overwrite = enabled;
      return this;
    }

    private CodecFactory codec() {
      String codec = config.getOrDefault(AVRO_COMPRESSION, AVRO_COMPRESSION_DEFAULT);
      try {
        return CodecName.valueOf(codec.toUpperCase(Locale.ENGLISH)).get();
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException("Unsupported compression codec: " + codec);
      }
    }

    public <D> FileAppender<D> build() throws IOException {
      Preconditions.checkNotNull(schema, "Schema is required");
      Preconditions.checkNotNull(name, "Table name is required and cannot be null");

      // add the Iceberg schema to keyValueMetadata
      meta("iceberg.schema", SchemaParser.toJson(schema));

      return new AvroFileAppender<>(
          AvroSchemaUtil.convert(schema, name), file, createWriterFunc, codec(), metadata, overwrite);
    }
  }

  public static ReadBuilder read(InputFile file) {
    return new ReadBuilder(file);
  }

  public static class ReadBuilder {
    private final ClassLoader defaultLoader = Thread.currentThread().getContextClassLoader();
    private final InputFile file;
    private final Map<String, String> renames = Maps.newLinkedHashMap();
    private boolean reuseContainers = false;
    private org.apache.iceberg.Schema schema = null;
    private Function<Schema, DatumReader<?>> createReaderFunc = readSchema -> {
      GenericAvroReader<?> reader = new GenericAvroReader<>(readSchema);
      reader.setClassLoader(defaultLoader);
      return reader;
    };
    private Long start = null;
    private Long length = null;

    private ReadBuilder(InputFile file) {
      Preconditions.checkNotNull(file, "Input file cannot be null");
      this.file = file;
    }

    public ReadBuilder createReaderFunc(Function<Schema, DatumReader<?>> readerFunction) {
      this.createReaderFunc = readerFunction;
      return this;
    }

    /**
     * Restricts the read to the given range: [start, end = start + length).
     *
     * @param newStart the start position for this read
     * @param newLength the length of the range this read should scan
     * @return this builder for method chaining
     */
    public ReadBuilder split(long newStart, long newLength) {
      this.start = newStart;
      this.length = newLength;
      return this;
    }

    public ReadBuilder project(org.apache.iceberg.Schema projectedSchema) {
      this.schema = projectedSchema;
      return this;
    }

    public ReadBuilder reuseContainers() {
      this.reuseContainers = true;
      return this;
    }

    public ReadBuilder reuseContainers(boolean shouldReuse) {
      this.reuseContainers = shouldReuse;
      return this;
    }

    public ReadBuilder rename(String fullName, String newName) {
      renames.put(fullName, newName);
      return this;
    }

    public <D> AvroIterable<D> build() {
      Preconditions.checkNotNull(schema, "Schema is required");
      return new AvroIterable<>(file,
          new ProjectionDatumReader<>(createReaderFunc, schema, renames),
          start, length, reuseContainers);
    }
  }

}
