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

  private static final byte[] MAGIC_NUM = new byte[] {'a', 'V', 'R', 'O'};

  private static byte[] encodeInt(int value) {
    return ByteBuffer.allocate(4).putInt(value).array();
  }

  private static int decodeInt(byte[] value) {
    return ByteBuffer.wrap(value).getInt();
  }

  public static <T> byte[] encode(T datum, Schema avroSchema) throws IOException {
    try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      out.write(MAGIC_NUM);

      byte[] avroSchemaBytes = avroSchema.toString().getBytes(StandardCharsets.UTF_8);
      out.write(encodeInt(avroSchemaBytes.length));
      out.write(avroSchemaBytes);

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
      Preconditions.checkState(in.read(buffer4) == 4, "Size of magic bytes isn't 4.");
      Preconditions.checkState(Arrays.equals(MAGIC_NUM, buffer4), "Magic bytes mismatched.");

      Preconditions.checkState(in.read(buffer4) == 4, "Could not read an integer from input stream.");
      int avroSchemeLength = decodeInt(buffer4);

      byte[] avroSchemaBytes = new byte[avroSchemeLength];
      Preconditions.checkState(in.read(avroSchemaBytes) == avroSchemeLength,
          "The length of read bytes is not the expected %s", avroSchemeLength);
      String avroSchemaString = new String(avroSchemaBytes, StandardCharsets.UTF_8);
      Schema avroSchema = new Schema.Parser().parse(avroSchemaString);

      BinaryDecoder binaryDecoder = DecoderFactory.get().binaryDecoder(in, null);
      DatumReader<T> reader = new GenericAvroReader<>(avroSchema);
      reader.setSchema(avroSchema);
      return reader.read(null, binaryDecoder);
    }
  }
}
