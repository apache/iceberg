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
package org.apache.iceberg.parquet;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.io.ParquetDecodingException;

/**
 * Implements a {@link ValuesReader} specifically to read given number of bytes from the underlying
 * {@link ByteBufferInputStream}.
 */
public class ValuesAsBytesReader extends ValuesReader {
  private ByteBufferInputStream valuesInputStream = null;
  // Only used for booleans.
  private int bitOffset;
  private byte currentByte = 0;

  public ValuesAsBytesReader() {}

  @Override
  public void initFromPage(int valueCount, ByteBufferInputStream in) {
    this.valuesInputStream = in;
  }

  @Override
  public void skip() {
    throw new UnsupportedOperationException();
  }

  public ByteBuffer getBuffer(int length) {
    try {
      return valuesInputStream.slice(length).order(ByteOrder.LITTLE_ENDIAN);
    } catch (IOException e) {
      throw new ParquetDecodingException("Failed to read " + length + " bytes", e);
    }
  }

  @Override
  public final int readInteger() {
    return getBuffer(4).getInt();
  }

  @Override
  public final long readLong() {
    return getBuffer(8).getLong();
  }

  @Override
  public final float readFloat() {
    return getBuffer(4).getFloat();
  }

  @Override
  public final double readDouble() {
    return getBuffer(8).getDouble();
  }

  @Override
  public final boolean readBoolean() {
    if (bitOffset == 0) {
      currentByte = getByte();
    }

    boolean value = (currentByte & (1 << bitOffset)) != 0;
    bitOffset += 1;
    if (bitOffset == 8) {
      bitOffset = 0;
    }
    return value;
  }

  /** Returns 1 if true, 0 otherwise. */
  public final int readBooleanAsInt() {
    if (bitOffset == 0) {
      currentByte = getByte();
    }
    int value = (currentByte & (1 << bitOffset)) >> bitOffset;
    bitOffset += 1;
    if (bitOffset == 8) {
      bitOffset = 0;
    }
    return value;
  }

  private byte getByte() {
    try {
      return (byte) valuesInputStream.read();
    } catch (IOException e) {
      throw new ParquetDecodingException("Failed to read a byte", e);
    }
  }
}
