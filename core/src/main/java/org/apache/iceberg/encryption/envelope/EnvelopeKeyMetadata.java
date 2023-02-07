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
package org.apache.iceberg.encryption.envelope;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.iceberg.encryption.EncryptionKeyMetadata;

public class EnvelopeKeyMetadata implements EncryptionKeyMetadata {

  public static final byte magic = 'I';
  public static final byte version = 0;

  private final String wrappingKeyId;
  private final ByteBuffer encryptionKey;
  private final ByteBuffer aadPrefix;

  public EnvelopeKeyMetadata(String wrappingKeyId, ByteBuffer encryptionKey, ByteBuffer aadPrefix) {
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
    try {
      AvroKeyRecord.Builder avroKeyRecordBuilder =
          AvroKeyRecord.newBuilder().setEncryptionKey(encryptionKey);
      if (null != wrappingKeyId) {
        avroKeyRecordBuilder.setWrappingKeyId(wrappingKeyId);
      }
      if (null != aadPrefix) {
        avroKeyRecordBuilder.setAadPrefix(aadPrefix);
      }
      AvroKeyRecord avroKeyRecord = avroKeyRecordBuilder.build();

      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      baos.write(magic);
      baos.write(version);

      DatumWriter<AvroKeyRecord> writer = new SpecificDatumWriter<>(AvroKeyRecord.class);
      BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(baos, null);
      writer.write(avroKeyRecord, encoder);
      encoder.flush();
      baos.flush();

      return ByteBuffer.wrap(baos.toByteArray());
    } catch (IOException e) {
      throw new RuntimeException("Failed to serialize envelope key metadata", e);
    }
  }

  @Override
  public EncryptionKeyMetadata copy() {
    EnvelopeKeyMetadata metadata =
        new EnvelopeKeyMetadata(wrappingKeyId(), encryptionKey(), aadPrefix());
    return metadata;
  }

  public static EnvelopeKeyMetadata parse(ByteBuffer buffer) {
    try {
      DatumReader<AvroKeyRecord> avroReader = new SpecificDatumReader<>(AvroKeyRecord.class);
      byte[] array = buffer.array();
      // TODO rm magic, mv version to Avro field?
      if (array[0] != magic) {
        throw new RuntimeException("Not envelope key metadata format");
      }
      if (array[1] != version) {
        throw new RuntimeException("Wrong version of envelope key metadata format: " + array[1]);
      }
      BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(array, 2, array.length - 2, null);
      AvroKeyRecord avroKeyRecord = avroReader.read(null, decoder);

      String wrappingKeyId = null;
      if (avroKeyRecord.getWrappingKeyId() != null) {
        wrappingKeyId = avroKeyRecord.getWrappingKeyId().toString();
      }
      return new EnvelopeKeyMetadata(
          wrappingKeyId, avroKeyRecord.getEncryptionKey(), avroKeyRecord.getAadPrefix());

    } catch (IOException e) {
      throw new RuntimeException("Failed to parse envelope encryption metadata", e);
    }
  }
}
