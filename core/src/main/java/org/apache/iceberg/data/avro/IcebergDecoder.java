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
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Map;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaNormalization;
import org.apache.avro.message.BadHeaderException;
import org.apache.avro.message.MessageDecoder;
import org.apache.avro.message.MissingSchemaException;
import org.apache.avro.message.SchemaStore;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.relocated.com.google.common.collect.MapMaker;

public class IcebergDecoder<D> extends MessageDecoder.BaseDecoder<D> {
  private static final ThreadLocal<byte[]> HEADER_BUFFER =
      ThreadLocal.withInitial(() -> new byte[10]);

  private static final ThreadLocal<ByteBuffer> FP_BUFFER =
      ThreadLocal.withInitial(
          () -> {
            byte[] header = HEADER_BUFFER.get();
            return ByteBuffer.wrap(header).order(ByteOrder.LITTLE_ENDIAN);
          });

  private final org.apache.iceberg.Schema readSchema;
  private final SchemaStore resolver;
  private final Map<Long, RawDecoder<D>> decoders = new MapMaker().makeMap();

  /**
   * Creates a new decoder that constructs datum instances described by an {@link
   * org.apache.iceberg.Schema Iceberg schema}.
   *
   * <p>The {@code readSchema} is as used the expected schema (read schema). Datum instances created
   * by this class will are described by the expected schema.
   *
   * <p>The schema used to decode incoming buffers is determined by the schema fingerprint encoded
   * in the message header. This class can decode messages that were encoded using the {@code
   * readSchema} and other schemas that are added using {@link
   * #addSchema(org.apache.iceberg.Schema)}.
   *
   * @param readSchema the schema used to construct datum instances
   */
  public IcebergDecoder(org.apache.iceberg.Schema readSchema) {
    this(readSchema, null);
  }

  /**
   * Creates a new decoder that constructs datum instances described by an {@link
   * org.apache.iceberg.Schema Iceberg schema}.
   *
   * <p>The {@code readSchema} is as used the expected schema (read schema). Datum instances created
   * by this class will are described by the expected schema.
   *
   * <p>The schema used to decode incoming buffers is determined by the schema fingerprint encoded
   * in the message header. This class can decode messages that were encoded using the {@code
   * readSchema} and other schemas that are added using {@link
   * #addSchema(org.apache.iceberg.Schema)}.
   *
   * <p>Schemas may also be returned from an Avro {@link SchemaStore}. Avro Schemas from the store
   * must be compatible with Iceberg and should contain id properties and use only Iceberg types.
   *
   * @param readSchema the {@link Schema} used to construct datum instances
   * @param resolver a {@link SchemaStore} used to find schemas by fingerprint
   */
  public IcebergDecoder(org.apache.iceberg.Schema readSchema, SchemaStore resolver) {
    this.readSchema = readSchema;
    this.resolver = resolver;
    addSchema(this.readSchema);
  }

  /**
   * Adds an {@link org.apache.iceberg.Schema Iceberg schema} that can be used to decode buffers.
   *
   * @param writeSchema a schema to use when decoding buffers
   */
  public void addSchema(org.apache.iceberg.Schema writeSchema) {
    addSchema(AvroSchemaUtil.convert(writeSchema, "table"));
  }

  private void addSchema(Schema writeSchema) {
    long fp = SchemaNormalization.parsingFingerprint64(writeSchema);
    RawDecoder decoder =
        new RawDecoder<>(
            readSchema, avroSchema -> DataReader.create(readSchema, avroSchema), writeSchema);
    decoders.put(fp, decoder);
  }

  private RawDecoder<D> getDecoder(long fp) {
    RawDecoder<D> decoder = decoders.get(fp);
    if (decoder != null) {
      return decoder;
    }

    if (resolver != null) {
      Schema writeSchema = resolver.findByFingerprint(fp);
      if (writeSchema != null) {
        addSchema(writeSchema);
        return decoders.get(fp);
      }
    }

    throw new MissingSchemaException("Cannot resolve schema for fingerprint: " + fp);
  }

  @Override
  public D decode(InputStream stream, D reuse) throws IOException {
    byte[] header = HEADER_BUFFER.get();
    try {
      if (!readFully(stream, header)) {
        throw new BadHeaderException("Not enough header bytes");
      }
    } catch (IOException e) {
      throw new IOException("Failed to read header and fingerprint bytes", e);
    }

    if (IcebergEncoder.V1_HEADER[0] != header[0] || IcebergEncoder.V1_HEADER[1] != header[1]) {
      throw new BadHeaderException(
          String.format("Unrecognized header bytes: 0x%02X 0x%02X", header[0], header[1]));
    }

    RawDecoder<D> decoder = getDecoder(FP_BUFFER.get().getLong(2));

    try {
      return decoder.decode(stream, reuse);
    } catch (UncheckedIOException e) {
      throw new AvroRuntimeException(e);
    }
  }

  /**
   * Reads a buffer from a stream, making multiple read calls if necessary.
   *
   * @param stream an InputStream to read from
   * @param bytes a buffer
   * @return true if the buffer is complete, false otherwise (stream ended)
   * @throws IOException if there is an error while reading
   */
  @SuppressWarnings("checkstyle:InnerAssignment")
  private boolean readFully(InputStream stream, byte[] bytes) throws IOException {
    int pos = 0;
    int bytesRead;
    while ((bytes.length - pos) > 0
        && (bytesRead = stream.read(bytes, pos, bytes.length - pos)) > 0) {
      pos += bytesRead;
    }
    return pos == bytes.length;
  }
}
