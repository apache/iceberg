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
package org.apache.iceberg;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.ByteBuffers;

/** Iceberg file format metrics. */
public class Metrics implements Serializable {

  private Long rowCount = null;
  private Map<Integer, Long> columnSizes = null;
  private Map<Integer, Long> valueCounts = null;
  private Map<Integer, Long> nullValueCounts = null;
  private Map<Integer, Long> nanValueCounts = null;
  private Map<Integer, ByteBuffer> lowerBounds = null;
  private Map<Integer, ByteBuffer> upperBounds = null;

  public Metrics() {}

  public Metrics(
      Long rowCount,
      Map<Integer, Long> columnSizes,
      Map<Integer, Long> valueCounts,
      Map<Integer, Long> nullValueCounts,
      Map<Integer, Long> nanValueCounts) {
    this.rowCount = rowCount;
    this.columnSizes = columnSizes;
    this.valueCounts = valueCounts;
    this.nullValueCounts = nullValueCounts;
    this.nanValueCounts = nanValueCounts;
  }

  public Metrics(
      Long rowCount,
      Map<Integer, Long> columnSizes,
      Map<Integer, Long> valueCounts,
      Map<Integer, Long> nullValueCounts,
      Map<Integer, Long> nanValueCounts,
      Map<Integer, ByteBuffer> lowerBounds,
      Map<Integer, ByteBuffer> upperBounds) {
    this.rowCount = rowCount;
    this.columnSizes = columnSizes;
    this.valueCounts = valueCounts;
    this.nullValueCounts = nullValueCounts;
    this.nanValueCounts = nanValueCounts;
    this.lowerBounds = lowerBounds;
    this.upperBounds = upperBounds;
  }

  /**
   * Get the number of records (rows) in file.
   *
   * @return the count of records (rows) in the file as a long
   */
  public Long recordCount() {
    return rowCount;
  }

  /**
   * Get the number of bytes for all fields in a file.
   *
   * @return a Map of fieldId to the size in bytes
   */
  public Map<Integer, Long> columnSizes() {
    return columnSizes;
  }

  /**
   * Get the number of all values, including nulls, NaN and repeated.
   *
   * @return a Map of fieldId to the number of all values including nulls, NaN and repeated
   */
  public Map<Integer, Long> valueCounts() {
    return valueCounts;
  }

  /**
   * Get the number of null values for all fields in a file.
   *
   * @return a Map of fieldId to the number of nulls
   */
  public Map<Integer, Long> nullValueCounts() {
    return nullValueCounts;
  }

  /**
   * Get the number of NaN values for all float and double fields in a file.
   *
   * @return a Map of fieldId to the number of NaN counts
   */
  public Map<Integer, Long> nanValueCounts() {
    return nanValueCounts;
  }

  /**
   * Get the non-null lower bound values for all fields in a file.
   *
   * <p>To convert the {@link ByteBuffer} back to a value, use {@link
   * org.apache.iceberg.types.Conversions#fromByteBuffer}.
   *
   * @return a Map of fieldId to the lower bound value as a ByteBuffer
   * @see <a href="https://iceberg.apache.org/spec/#appendix-d-single-value-serialization">Iceberg
   *     Spec - Appendix D: Single-value serialization</a>
   */
  public Map<Integer, ByteBuffer> lowerBounds() {
    return lowerBounds;
  }

  /**
   * Get the non-null upper bound values for all fields in a file.
   *
   * @return a Map of fieldId to the upper bound value as a ByteBuffer
   */
  public Map<Integer, ByteBuffer> upperBounds() {
    return upperBounds;
  }

  /**
   * Implemented the method to enable serialization of ByteBuffers.
   *
   * @param out The stream where to write
   * @throws IOException On serialization error
   */
  private void writeObject(ObjectOutputStream out) throws IOException {
    out.writeObject(rowCount);
    out.writeObject(columnSizes);
    out.writeObject(valueCounts);
    out.writeObject(nullValueCounts);
    out.writeObject(nanValueCounts);

    writeByteBufferMap(out, lowerBounds);
    writeByteBufferMap(out, upperBounds);
  }

  private static void writeByteBufferMap(
      ObjectOutputStream out, Map<Integer, ByteBuffer> byteBufferMap) throws IOException {
    if (byteBufferMap == null) {
      out.writeInt(-1);

    } else {
      // Write the size
      out.writeInt(byteBufferMap.size());

      for (Map.Entry<Integer, ByteBuffer> entry : byteBufferMap.entrySet()) {
        // Write the key and the value converted to byte[]
        out.writeObject(entry.getKey());
        out.writeObject(ByteBuffers.toByteArray(entry.getValue()));
      }
    }
  }

  /**
   * Implemented the method to enable deserialization of ByteBuffers.
   *
   * @param in The stream to read from
   * @throws IOException On serialization error
   * @throws ClassNotFoundException If the class is not found
   */
  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    rowCount = (Long) in.readObject();
    columnSizes = (Map<Integer, Long>) in.readObject();
    valueCounts = (Map<Integer, Long>) in.readObject();
    nullValueCounts = (Map<Integer, Long>) in.readObject();
    nanValueCounts = (Map<Integer, Long>) in.readObject();

    lowerBounds = readByteBufferMap(in);
    upperBounds = readByteBufferMap(in);
  }

  @SuppressWarnings("DangerousJavaDeserialization")
  private static Map<Integer, ByteBuffer> readByteBufferMap(ObjectInputStream in)
      throws IOException, ClassNotFoundException {
    int size = in.readInt();

    if (size == -1) {
      return null;

    } else {
      Map<Integer, ByteBuffer> result = Maps.newHashMapWithExpectedSize(size);

      for (int i = 0; i < size; ++i) {
        Integer key = (Integer) in.readObject();
        byte[] data = (byte[]) in.readObject();

        if (data != null) {
          result.put(key, ByteBuffer.wrap(data));
        } else {
          result.put(key, null);
        }
      }

      return result;
    }
  }
}
