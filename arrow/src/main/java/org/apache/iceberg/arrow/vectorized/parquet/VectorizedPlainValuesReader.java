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
package org.apache.iceberg.arrow.vectorized.parquet;

import java.nio.ByteBuffer;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.FieldVector;
import org.apache.iceberg.parquet.ValuesAsBytesReader;
import org.apache.parquet.io.api.Binary;

class VectorizedPlainValuesReader extends ValuesAsBytesReader implements VectorizedValuesReader {

  VectorizedPlainValuesReader() {}

  @Override
  public byte readByte() {
    return (byte) readInteger();
  }

  @Override
  public short readShort() {
    return (short) readInteger();
  }

  @Override
  public Binary readBinary(int len) {
    ByteBuffer buffer = getBuffer(len);
    if (buffer.hasArray()) {
      return Binary.fromConstantByteArray(
          buffer.array(), buffer.arrayOffset() + buffer.position(), len);
    } else {
      byte[] bytes = new byte[len];
      buffer.get(bytes);
      return Binary.fromConstantByteArray(bytes);
    }
  }

  private void readValues(int total, FieldVector vec, int rowId, int typeWidth) {
    ByteBuffer buffer = getBuffer(total * typeWidth);
    vec.getDataBuffer().setBytes((long) rowId * typeWidth, buffer);
  }

  @Override
  public void readIntegers(int total, FieldVector vec, int rowId) {
    readValues(total, vec, rowId, INT_SIZE);
  }

  @Override
  public void readLongs(int total, FieldVector vec, int rowId) {
    readValues(total, vec, rowId, LONG_SIZE);
  }

  @Override
  public void readFloats(int total, FieldVector vec, int rowId) {
    readValues(total, vec, rowId, FLOAT_SIZE);
  }

  @Override
  public void readDoubles(int total, FieldVector vec, int rowId) {
    readValues(total, vec, rowId, DOUBLE_SIZE);
  }

  @Override
  public void readBinary(int total, FieldVector vec, int rowId, boolean setArrowValidityVector) {
    int len = readInteger();
    ByteBuffer buffer = readBinary(len).toByteBuffer();
    // Calling setValueLengthSafe takes care of allocating a larger buffer if
    // running out of space.
    ((BaseVariableWidthVector) vec).setValueLengthSafe(rowId, len);
    int startOffset = ((BaseVariableWidthVector) vec).getStartOffset(rowId);
    // It is possible that the data buffer was reallocated. So it is important to
    // not cache the data buffer reference but instead use vector.getDataBuffer().
    vec.getDataBuffer().setBytes(startOffset, buffer);
    // Similarly, we need to get the latest reference to the validity buffer as well
    // since reallocation changes reference of the validity buffers as well.
    if (setArrowValidityVector) {
      BitVectorHelper.setBit(vec.getValidityBuffer(), rowId);
    }
  }
}
