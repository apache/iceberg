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
import java.io.UncheckedIOException;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.message.MessageDecoder;
import org.apache.iceberg.avro.GenericAvroReader;
import org.apache.iceberg.data.avro.RawDecoder;
import org.apache.iceberg.relocated.com.google.common.collect.MapMaker;

class KeyMetadataDecoder extends MessageDecoder.BaseDecoder<StandardKeyMetadata> {
  private final org.apache.iceberg.Schema readSchema;
  private final Map<Byte, RawDecoder<StandardKeyMetadata>> decoders = new MapMaker().makeMap();

  /**
   * Creates a new decoder that constructs key metadata instances described by schema version.
   *
   * <p>The {@code readSchemaVersion} is as used the version of the expected (read) schema. Datum
   * instances created by this class will are described by the expected schema.
   */
  KeyMetadataDecoder(byte readSchemaVersion) {
    this.readSchema = StandardKeyMetadata.supportedSchemaVersions().get(readSchemaVersion);
  }

  @Override
  public StandardKeyMetadata decode(InputStream stream, StandardKeyMetadata reuse) {
    byte writeSchemaVersion;

    try {
      writeSchemaVersion = (byte) stream.read();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read the version byte", e);
    }

    if (writeSchemaVersion < 0) {
      throw new RuntimeException("Version byte - end of stream reached");
    }

    Schema writeSchema = StandardKeyMetadata.supportedAvroSchemaVersions().get(writeSchemaVersion);

    if (writeSchema == null) {
      throw new UnsupportedOperationException(
          "Cannot resolve schema for version: " + writeSchemaVersion);
    }

    RawDecoder<StandardKeyMetadata> decoder = decoders.get(writeSchemaVersion);

    if (decoder == null) {
      decoder = new RawDecoder<>(readSchema, GenericAvroReader::create, writeSchema);

      decoders.put(writeSchemaVersion, decoder);
    }

    return decoder.decode(stream, reuse);
  }
}
