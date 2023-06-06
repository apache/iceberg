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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.avro.generic.IndexedRecord;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;

class KeyMetadata implements EncryptionKeyMetadata, IndexedRecord {
  private static final byte V1 = 1;
  private static final Schema SCHEMA_V1 =
      new Schema(
          required(0, "encryption_key", Types.BinaryType.get()),
          optional(1, "aad_prefix", Types.BinaryType.get()));
  private static final org.apache.avro.Schema AVRO_SCHEMA_V1 =
      AvroSchemaUtil.convert(SCHEMA_V1, KeyMetadata.class.getCanonicalName());

  private static final Map<Byte, Schema> schemaVersions = ImmutableMap.of(V1, SCHEMA_V1);
  private static final Map<Byte, org.apache.avro.Schema> avroSchemaVersions =
      ImmutableMap.of(V1, AVRO_SCHEMA_V1);

  private static final KeyMetadataEncoder KEY_METADATA_ENCODER = new KeyMetadataEncoder(V1);
  private static final KeyMetadataDecoder KEY_METADATA_DECODER = new KeyMetadataDecoder(V1);

  private ByteBuffer encryptionKey;
  private ByteBuffer aadPrefix;
  private org.apache.avro.Schema avroSchema;

  /** Used by Avro reflection to instantiate this class * */
  KeyMetadata() {}

  KeyMetadata(ByteBuffer encryptionKey, ByteBuffer aadPrefix) {
    this.encryptionKey = encryptionKey;
    this.aadPrefix = aadPrefix;
    this.avroSchema = AVRO_SCHEMA_V1;
  }

  static Map<Byte, Schema> supportedSchemaVersions() {
    return schemaVersions;
  }

  static Map<Byte, org.apache.avro.Schema> supportedAvroSchemaVersions() {
    return avroSchemaVersions;
  }

  ByteBuffer encryptionKey() {
    return encryptionKey;
  }

  ByteBuffer aadPrefix() {
    return aadPrefix;
  }

  static KeyMetadata parse(ByteBuffer buffer) {
    try {
      return KEY_METADATA_DECODER.decode(buffer);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to parse envelope encryption metadata", e);
    }
  }

  @Override
  public ByteBuffer buffer() {
    try {
      return KEY_METADATA_ENCODER.encode(this);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to serialize envelope key metadata", e);
    }
  }

  @Override
  public EncryptionKeyMetadata copy() {
    KeyMetadata metadata = new KeyMetadata(encryptionKey(), aadPrefix());
    return metadata;
  }

  @Override
  public void put(int i, Object v) {
    switch (i) {
      case 0:
        this.encryptionKey = (ByteBuffer) v;
        return;
      case 1:
        this.aadPrefix = (ByteBuffer) v;
        return;
      default:
        // ignore the object, it must be from a newer version of the format
    }
  }

  @Override
  public Object get(int i) {
    switch (i) {
      case 0:
        return encryptionKey;
      case 1:
        return aadPrefix;
      default:
        throw new UnsupportedOperationException("Unknown field ordinal: " + i);
    }
  }

  @Override
  public org.apache.avro.Schema getSchema() {
    return avroSchema;
  }
}
