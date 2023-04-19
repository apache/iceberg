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
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
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

  private AvroEncoderUtil() {}

  static {
    LogicalTypes.register(LogicalMap.NAME, schema -> LogicalMap.get());
  }

  private static final byte[] MAGIC_BYTES = new byte[] {(byte) 0xC2, (byte) 0x01};

  public static <T> byte[] encode(T datum, Schema avroSchema) throws IOException {
    try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      DataOutputStream dataOut = new DataOutputStream(out);

      // Write the magic bytes
      dataOut.write(MAGIC_BYTES);

      // Write avro schema
      dataOut.writeUTF(avroSchema.toString());

      // Encode the datum with avro schema.
      BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
      DatumWriter<T> writer = new GenericAvroWriter<>(avroSchema);
      writer.write(datum, encoder);
      encoder.flush();

      return out.toByteArray();
    }
  }

  public static <T> T decode(byte[] data) throws IOException {
    try (ByteArrayInputStream in = new ByteArrayInputStream(data, 0, data.length)) {
      DataInputStream dataInput = new DataInputStream(in);

      // Read the magic bytes
      byte header0 = dataInput.readByte();
      byte header1 = dataInput.readByte();
      Preconditions.checkState(
          header0 == MAGIC_BYTES[0] && header1 == MAGIC_BYTES[1],
          "Unrecognized header bytes: 0x%02X 0x%02X",
          header0,
          header1);

      // Read avro schema
      Schema avroSchema = new Schema.Parser().parse(dataInput.readUTF());

      // Decode the datum with the parsed avro schema.
      BinaryDecoder binaryDecoder = DecoderFactory.get().binaryDecoder(in, null);
      DatumReader<T> reader = new GenericAvroReader<>(avroSchema);
      reader.setSchema(avroSchema);
      return reader.read(null, binaryDecoder);
    }
  }
}
