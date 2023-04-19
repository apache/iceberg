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
import java.util.function.Function;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.message.MessageDecoder;
import org.apache.avro.message.MissingSchemaException;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.avro.GenericAvroReader;
import org.apache.iceberg.data.avro.RawDecoder;
import org.apache.iceberg.relocated.com.google.common.collect.MapMaker;

public class KeyMetadataDecoder<D> extends MessageDecoder.BaseDecoder<D> {
  private final org.apache.iceberg.Schema readSchema;
  private final Map<Byte, RawDecoder<D>> decoders = new MapMaker().makeMap();

  /**
   * Creates a new decoder that constructs key metadata instances described by schema version.
   *
   * <p>The {@code readSchemaVersion} is as used the version of the expected (read) schema. Datum
   * instances created by this class will are described by the expected schema.
   */
  public KeyMetadataDecoder(byte readSchemaVersion) {
    this.readSchema = KeyMetadata.supportedSchemaVersions.get(readSchemaVersion);
  }

  @Override
  public D decode(InputStream stream, D reuse) throws IOException {
    byte writeSchemaVersion;
    try {
      writeSchemaVersion = (byte) stream.read();
    } catch (IOException e) {
      throw new IOException("Failed to read the version byte", e);
    }

    if (writeSchemaVersion < 0) {
      throw new IOException("Version byte - end of stream reached");
    }

    org.apache.iceberg.Schema writeSchema =
        KeyMetadata.supportedSchemaVersions.get(writeSchemaVersion);

    if (writeSchema == null) {
      throw new MissingSchemaException("Cannot resolve schema for version: " + writeSchemaVersion);
    }

    RawDecoder<D> decoder = decoders.get(writeSchemaVersion);
    if (decoder == null) {
      Function<Schema, DatumReader<?>> createReaderFunc =
          schema -> {
            GenericAvroReader<?> reader = GenericAvroReader.create(schema);
            return reader;
          };

      decoder =
          new RawDecoder<>(
              readSchema,
              createReaderFunc,
              AvroSchemaUtil.convert(writeSchema, KeyMetadata.class.getCanonicalName()));

      decoders.put(writeSchemaVersion, decoder);
    }

    return decoder.decode(stream, reuse);
  }
}
