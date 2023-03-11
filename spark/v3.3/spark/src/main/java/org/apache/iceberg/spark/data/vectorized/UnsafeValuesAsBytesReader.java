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
package org.apache.iceberg.spark.data.vectorized;

import java.nio.ByteBuffer;
import org.apache.iceberg.parquet.ValuesAsBytesReader;
import org.apache.spark.unsafe.Platform;

public class UnsafeValuesAsBytesReader extends ValuesAsBytesReader {

  private static int BYTE_ARRAY_OFFSET = Platform.BYTE_ARRAY_OFFSET;

  @Override
  public int readInteger() {
    ByteBuffer buffer = getBuffer(4);
    if (buffer.hasArray()) {
      long offset = buffer.arrayOffset() + buffer.position() + Platform.BYTE_ARRAY_OFFSET;
      return Platform.getInt(buffer.array(), offset);
    } else {
      return buffer.getInt();
    }
  }

  @Override
  public long readLong() {
    ByteBuffer buffer = getBuffer(8);
    if (buffer.hasArray()) {
      long offset = buffer.arrayOffset() + buffer.position() + Platform.BYTE_ARRAY_OFFSET;
      return Platform.getLong(buffer.array(), offset);
    } else {
      return buffer.getLong();
    }
  }

  public boolean supportUnsafe() {
    return true;
  }

  public int getByteArrayOffset() {
    return BYTE_ARRAY_OFFSET;
  }

  public int getUnsafeInt(Object object, long offset) {
    return Platform.getInt(object, offset);
  }

  public long getUnsafeLong(Object object, long offset) {
    return Platform.getLong(object, offset);
  }
}
