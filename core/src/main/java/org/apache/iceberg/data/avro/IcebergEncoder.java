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
package org.apache.iceberg.data.avro;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.SchemaNormalization;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.message.MessageEncoder;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.relocated.com.google.common.primitives.Bytes;

public class IcebergEncoder<D> implements MessageEncoder<D> {

  static final byte[] V1_HEADER = new byte[] {(byte) 0xC3, (byte) 0x01};

  private static final ThreadLocal<BufferOutputStream> TEMP =
      ThreadLocal.withInitial(BufferOutputStream::new);

  private static final ThreadLocal<BinaryEncoder> ENCODER = new ThreadLocal<>();

  private final byte[] headerBytes;
  private final boolean copyOutputBytes;
  private final DatumWriter<D> writer;

  /**
   * Creates a new {@link MessageEncoder} that will deconstruct datum instances described by the
   * {@link Schema schema}.
   *
   * <p>Buffers returned by {@code encode} are copied and will not be modified by future calls to
   * {@code encode}.
   *
   * @param schema the {@link Schema} for datum instances
   */
  public IcebergEncoder(Schema schema) {
    this(schema, true);
  }

  /**
   * Creates a new {@link MessageEncoder} that will deconstruct datum instances described by the
   * {@link Schema schema}.
   *
   * <p>If {@code shouldCopy} is true, then buffers returned by {@code encode} are copied and will
   * not be modified by future calls to {@code encode}.
   *
   * <p>If {@code shouldCopy} is false, then buffers returned by {@code encode} wrap a thread-local
   * buffer that can be reused by future calls to {@code encode}, but may not be. Callers should
   * only set {@code shouldCopy} to false if the buffer will be copied before the current thread's
   * next call to {@code encode}.
   *
   * @param schema the {@link Schema} for datum instances
   * @param shouldCopy whether to copy buffers before returning encoded results
   */
  public IcebergEncoder(Schema schema, boolean shouldCopy) {
    this.copyOutputBytes = shouldCopy;
    org.apache.avro.Schema avroSchema = AvroSchemaUtil.convert(schema, "table");
    this.writer = DataWriter.create(avroSchema);
    this.headerBytes = getWriteHeader(avroSchema);
  }

  @Override
  public ByteBuffer encode(D datum) throws IOException {
    BufferOutputStream temp = TEMP.get();
    temp.reset();

    temp.write(headerBytes);
    encode(datum, temp);

    if (copyOutputBytes) {
      return temp.toBufferWithCopy();
    } else {
      return temp.toBufferWithoutCopy();
    }
  }

  @Override
  public void encode(D datum, OutputStream stream) throws IOException {
    BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(stream, ENCODER.get());
    ENCODER.set(encoder);
    writer.write(datum, encoder);
    encoder.flush();
  }

  private static class BufferOutputStream extends ByteArrayOutputStream {
    BufferOutputStream() {}

    ByteBuffer toBufferWithoutCopy() {
      return ByteBuffer.wrap(buf, 0, count);
    }

    ByteBuffer toBufferWithCopy() {
      return ByteBuffer.wrap(toByteArray());
    }
  }

  private static byte[] getWriteHeader(org.apache.avro.Schema schema) {
    try {
      byte[] fp = SchemaNormalization.parsingFingerprint("CRC-64-AVRO", schema);
      return Bytes.concat(V1_HEADER, fp);
    } catch (NoSuchAlgorithmException e) {
      throw new AvroRuntimeException(e);
    }
  }
}
