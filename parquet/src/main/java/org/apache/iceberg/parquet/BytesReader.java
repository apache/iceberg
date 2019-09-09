/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.parquet;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.arrow.vector.BigIntVector;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.io.ParquetDecodingException;

/**
 * Implements a {@link ValuesReader} specifically to read given number of bytes from the underlying
 * {@link ByteBufferInputStream}.
 */
public class BytesReader extends ValuesReader {
  private ByteBufferInputStream in = null;

  public BytesReader() {
  }

  @Override
  public void initFromPage(int valueCount, ByteBufferInputStream in) {
    this.in = in;
  }

  @Override
  public void skip() {
    throw new UnsupportedOperationException();
  }

  public ByteBuffer getBuffer(int length) {
    try {
      return in.slice(length).order(ByteOrder.LITTLE_ENDIAN);
    } catch (IOException e) {
      throw new ParquetDecodingException("Failed to read " + length + " bytes", e);
    }
  }

  @Override
  public final int readInteger() {
    return getBuffer(4).getInt();
  }

}

