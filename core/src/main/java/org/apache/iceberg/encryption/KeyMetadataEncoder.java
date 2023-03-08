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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.message.MessageEncoder;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.data.avro.DataWriter;

public class KeyMetadataEncoder<D> implements MessageEncoder<D> {
  private final byte schemaVersion;
  private final DatumWriter<D> writer;
  private final BufferOutputStream bufferOutputStream;
  private BinaryEncoder binaryEncoder;

  /**
   * Creates a new {@link MessageEncoder} that will deconstruct datum instances described by the
   * {@link Schema schema}.
   *
   * <p>Buffers returned by {@code encode} are copied and will not be modified by future calls to
   * {@code encode}.
   *
   * @param schema the {@link Schema} for datum instances
   */
  public KeyMetadataEncoder(byte version, Schema schema) {
    org.apache.avro.Schema avroSchema = AvroSchemaUtil.convert(schema, "key_metadata");
    this.writer = DataWriter.create(avroSchema);
    this.schemaVersion = version;
    this.bufferOutputStream = new BufferOutputStream();
  }

  @Override
  public ByteBuffer encode(D datum) throws IOException {
    bufferOutputStream.reset();

    bufferOutputStream.write(schemaVersion);
    encode(datum, bufferOutputStream);

    return bufferOutputStream.toBufferWithCopy();
  }

  @Override
  public void encode(D datum, OutputStream stream) throws IOException {
    binaryEncoder = EncoderFactory.get().directBinaryEncoder(stream, binaryEncoder);
    writer.write(datum, binaryEncoder);
    binaryEncoder.flush();
  }

  private static class BufferOutputStream extends ByteArrayOutputStream {
    BufferOutputStream() {}

    ByteBuffer toBufferWithCopy() {
      return ByteBuffer.wrap(toByteArray());
    }
  }
}
