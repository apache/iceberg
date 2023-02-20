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
import org.apache.avro.message.MessageDecoder;
import org.apache.avro.message.MessageEncoder;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.avro.IcebergDecoder;
import org.apache.iceberg.data.avro.IcebergEncoder;
import org.apache.iceberg.types.Types;

public class KeyMetadata implements EncryptionKeyMetadata {

  private static final int encryptionKeyPos = 0;
  private static final int wrappingKeyIdPos = 1;
  private static final int aadPrefixPos = 2;

  private static final Schema SCHEMA_V1 =
      new Schema(
          required(encryptionKeyPos, "encryptionKey", Types.BinaryType.get()),
          optional(wrappingKeyIdPos, "wrappingKeyId", Types.StringType.get()),
          optional(aadPrefixPos, "aadPrefix", Types.BinaryType.get()));

  private final String wrappingKeyId;
  private final ByteBuffer encryptionKey;
  private final ByteBuffer aadPrefix;

  public KeyMetadata(ByteBuffer encryptionKey, String wrappingKeyId, ByteBuffer aadPrefix) {
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

  @Override
  public ByteBuffer buffer() {
    Record rec = GenericRecord.create(SCHEMA_V1.asStruct());
    rec.set(encryptionKeyPos, encryptionKey);
    rec.set(wrappingKeyIdPos, wrappingKeyId);
    rec.set(aadPrefixPos, aadPrefix);

    MessageEncoder<Record> encoder = new IcebergEncoder<>(SCHEMA_V1);

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

  public static KeyMetadata parse(ByteBuffer buffer) {
    MessageDecoder<Record> decoder = new IcebergDecoder<>(SCHEMA_V1);
    try {
      Record rec = decoder.decode(buffer);
      return new KeyMetadata(
          rec.get(encryptionKeyPos, ByteBuffer.class),
          rec.get(wrappingKeyIdPos, String.class),
          rec.get(aadPrefixPos, ByteBuffer.class));
    } catch (IOException e) {
      throw new RuntimeException("Failed to parse envelope encryption metadata", e);
    }
  }
}
