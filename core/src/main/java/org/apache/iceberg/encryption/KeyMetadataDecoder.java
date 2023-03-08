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
package org.apache.iceberg.encryption;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.message.BadHeaderException;
import org.apache.avro.message.MessageDecoder;
import org.apache.avro.message.MissingSchemaException;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.avro.ProjectionDatumReader;
import org.apache.iceberg.data.avro.DataReader;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.MapMaker;

public class KeyMetadataDecoder<D> extends MessageDecoder.BaseDecoder<D> {
  private final org.apache.iceberg.Schema readSchema;
  private final Map<Byte, RawDecoder<D>> decoders = new MapMaker().makeMap();
  private final byte[] versionBuffer;

  /**
   * Creates a new decoder that constructs datum instances described by an {@link
   * org.apache.iceberg.Schema Iceberg schema}.
   *
   * <p>The {@code readSchema} is as used the expected schema (read schema). Datum instances created
   * by this class will are described by the expected schema.
   *
   * @param readSchema the schema used to construct datum instances
   */
  public KeyMetadataDecoder(byte currentSchemaVersion, org.apache.iceberg.Schema readSchema) {
    this.readSchema = readSchema;
    addSchema(currentSchemaVersion, this.readSchema);
    this.versionBuffer = new byte[1];
  }

  /**
   * Adds an {@link org.apache.iceberg.Schema Iceberg schema} that can be used to decode buffers.
   *
   * @param writeSchema a schema to use when decoding buffers
   */
  public void addSchema(byte schemaVersion, org.apache.iceberg.Schema writeSchema) {
    decoders.put(
        schemaVersion,
        new KeyMetadataDecoder.RawDecoder<>(
            readSchema, AvroSchemaUtil.convert(writeSchema, "key_metadata")));
  }

  @Override
  public D decode(InputStream stream, D reuse) throws IOException {
    try {
      if (!readFully(stream, versionBuffer)) {
        throw new BadHeaderException("Failed to read version - no bytes in stream");
      }
    } catch (IOException e) {
      throw new IOException("Failed to read the version byte", e);
    }

    byte writeSchemaVersion = versionBuffer[0];

    RawDecoder<D> decoder = decoders.get(writeSchemaVersion);

    if (decoder == null) {
      throw new MissingSchemaException("Cannot resolve schema for version: " + writeSchemaVersion);
    }

    return decoder.decode(stream, reuse);
  }

  private static class RawDecoder<D> extends BaseDecoder<D> {
    private final DatumReader<D> reader;
    private BinaryDecoder binaryDecoder;

    /**
     * Creates a new {@link MessageDecoder} that constructs datum instances described by the {@link
     * Schema readSchema}.
     *
     * <p>The {@code readSchema} is used for the expected schema and the {@code writeSchema} is the
     * schema used to decode buffers. The {@code writeSchema} must be the schema that was used to
     * encode all buffers decoded by this class.
     *
     * @param readSchema the schema used to construct datum instances
     * @param writeSchema the schema used to decode buffers
     */
    private RawDecoder(org.apache.iceberg.Schema readSchema, Schema writeSchema) {
      this.reader =
          new ProjectionDatumReader<>(
              avroSchema -> DataReader.create(readSchema, avroSchema),
              readSchema,
              ImmutableMap.of(),
              null);
      this.reader.setSchema(writeSchema);
    }

    @Override
    public D decode(InputStream stream, D reuse) {
      binaryDecoder = DecoderFactory.get().directBinaryDecoder(stream, binaryDecoder);
      try {
        return reader.read(reuse, binaryDecoder);
      } catch (IOException e) {
        throw new AvroRuntimeException("Decoding datum failed", e);
      }
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
