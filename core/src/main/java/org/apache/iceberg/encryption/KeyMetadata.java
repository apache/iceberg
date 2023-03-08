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
import java.nio.ByteBuffer;
import org.apache.avro.generic.IndexedRecord;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;

public class KeyMetadata implements EncryptionKeyMetadata, IndexedRecord {
  private static final ThreadLocal<KeyMetadataEncoder<Record>> ENCODER = new ThreadLocal<>();
  private static final ThreadLocal<KeyMetadataDecoder<Record>> DECODER = new ThreadLocal<>();

  private static final String encryptionKeyField = "encryption_key";
  private static final String wrappingKeyIdField = "wrapping_key_id";
  private static final String aadPrefixField = "aad_prefix";

  static final byte V1 = 1;
  private static final Schema SCHEMA_V1 =
      new Schema(
          required(0, encryptionKeyField, Types.BinaryType.get()),
          optional(1, wrappingKeyIdField, Types.StringType.get()),
          optional(2, aadPrefixField, Types.BinaryType.get()));

  private String wrappingKeyId;
  private ByteBuffer encryptionKey;
  private ByteBuffer aadPrefix;

  KeyMetadata(ByteBuffer encryptionKey, String wrappingKeyId, ByteBuffer aadPrefix) {
    this.wrappingKeyId = wrappingKeyId;
    this.encryptionKey = encryptionKey;
    this.aadPrefix = aadPrefix;
  }

  public String wrappingKeyId() {
    return wrappingKeyId;
  }

  public ByteBuffer encryptionKey() {
    return encryptionKey;
  }

  public ByteBuffer aadPrefix() {
    return aadPrefix;
  }

  public static KeyMetadata parse(ByteBuffer buffer) {
    KeyMetadataDecoder<Record> decoder = DECODER.get();
    if (decoder == null) {
      decoder = new KeyMetadataDecoder<>(V1, SCHEMA_V1);
      DECODER.set(decoder);
    }
    try {
      Record rec = decoder.decode(buffer);
      return new KeyMetadata(
          (ByteBuffer) rec.getField(encryptionKeyField),
          (String) rec.getField(wrappingKeyIdField),
          (ByteBuffer) rec.getField(aadPrefixField));
    } catch (IOException e) {
      throw new RuntimeException("Failed to parse envelope encryption metadata", e);
    }
  }

  @Override
  public ByteBuffer buffer() {
    Record rec = GenericRecord.create(SCHEMA_V1.asStruct());
    rec.setField(encryptionKeyField, encryptionKey);
    rec.setField(wrappingKeyIdField, wrappingKeyId);
    rec.setField(aadPrefixField, aadPrefix);

    KeyMetadataEncoder<Record> encoder = ENCODER.get();
    if (encoder == null) {
      encoder = new KeyMetadataEncoder<>(V1, SCHEMA_V1);
      ENCODER.set(encoder);
    }

    try {
      return encoder.encode(rec);
    } catch (IOException e) {
      throw new RuntimeException("Failed to serialize envelope key metadata", e);
    }
  }

  @Override
  public EncryptionKeyMetadata copy() {
    KeyMetadata metadata = new KeyMetadata(encryptionKey(), wrappingKeyId(), aadPrefix());
    return metadata;
  }

  @Override
  public void put(int i, Object v) {
    switch (i) {
      case 0:
        this.encryptionKey = (ByteBuffer) v;
        return;
      case 1:
        this.wrappingKeyId = v.toString();
        return;
      case 2:
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
        return wrappingKeyId;
      case 2:
        return aadPrefix;
      default:
        throw new UnsupportedOperationException("Unknown field ordinal: " + i);
    }
  }

  @Override
  public org.apache.avro.Schema getSchema() {
    return AvroSchemaUtil.convert(SCHEMA_V1, "key_metadata");
  }
}
