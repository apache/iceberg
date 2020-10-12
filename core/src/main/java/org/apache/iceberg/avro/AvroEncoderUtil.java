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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public class AvroEncoderUtil {

  private AvroEncoderUtil() {
  }

  static {
    LogicalTypes.register(LogicalMap.NAME, schema -> LogicalMap.get());
  }

  private static final int VERSION = 1;
  private static final byte[] MAGIC_BYTES = new byte[] {'a', 'V', 'R', VERSION};

  private static byte[] encodeInt(int value) {
    return ByteBuffer.allocate(4).putInt(value).array();
  }

  private static int decodeInt(byte[] value) {
    return ByteBuffer.wrap(value).getInt();
  }

  public static <T> byte[] encode(T datum, Schema avroSchema) throws IOException {
    try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      // Write the magic bytes
      out.write(MAGIC_BYTES);

      // Write the length of avro schema string.
      byte[] avroSchemaBytes = avroSchema.toString().getBytes(StandardCharsets.UTF_8);
      out.write(encodeInt(avroSchemaBytes.length));

      // Write the avro schema string.
      out.write(avroSchemaBytes);

      // Encode the datum with avro schema.
      BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
      DatumWriter<T> writer = new GenericAvroWriter<>(avroSchema);
      writer.write(datum, encoder);
      encoder.flush();
      return out.toByteArray();
    }
  }

  public static <T> T decode(byte[] data) throws IOException {
    byte[] buffer4 = new byte[4];
    try (ByteArrayInputStream in = new ByteArrayInputStream(data, 0, data.length)) {
      // Read the magic bytes
      Preconditions.checkState(in.read(buffer4) == 4, "Size of magic bytes isn't 4.");
      Preconditions.checkState(Arrays.equals(MAGIC_BYTES, buffer4), "Magic bytes mismatched.");

      // Read the length of avro schema string.
      Preconditions.checkState(in.read(buffer4) == 4, "Could not read an integer from input stream.");
      int avroSchemaLength = decodeInt(buffer4);
      Preconditions.checkState(avroSchemaLength > 0, "Length of avro schema string should be positive");

      // Read the avro schema string.
      byte[] avroSchemaBytes = new byte[avroSchemaLength];
      Preconditions.checkState(in.read(avroSchemaBytes) == avroSchemaLength,
          "The length of read bytes is not the expected %s", avroSchemaLength);
      String avroSchemaString = new String(avroSchemaBytes, StandardCharsets.UTF_8);
      Schema avroSchema = new Schema.Parser().parse(avroSchemaString);

      // Decode the datum with the parsed avro schema.
      BinaryDecoder binaryDecoder = DecoderFactory.get().binaryDecoder(in, null);
      DatumReader<T> reader = new GenericAvroReader<>(avroSchema);
      reader.setSchema(avroSchema);
      return reader.read(null, binaryDecoder);
    }
  }
}
