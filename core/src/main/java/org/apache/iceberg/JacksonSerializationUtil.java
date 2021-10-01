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

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.iceberg.expressions.ResidualEvaluator;

/**
 * Util methods to help Jackson serialization of Iceberg objects, useful in systems like Presto and Trino.
 */
public class JacksonSerializationUtil {

  private JacksonSerializationUtil() {
  }

  public static FileScanTask createFileScanTask(DataFile file, DeleteFile[] deletes, String schemaString,
                                                String specString, ResidualEvaluator residuals) {
    return new BaseFileScanTask(file, deletes, schemaString, specString, residuals);
  }

  public static DataFile createDataFile(int specId, FileContent content, String filePath, FileFormat format,
                                        PartitionData partitionData, long fileSizeInBytes, long recordCount,
                                        Map<Integer, Long> columnSizes, Map<Integer, Long> valueCounts,
                                        Map<Integer, Long> nullValueCounts, Map<Integer, Long> nanValueCounts,
                                        Map<Integer, ByteBuffer> lowerBounds, Map<Integer, ByteBuffer> upperBounds,
                                        List<Long> splitOffsets, int[] equalityFieldIds, Integer sortOrderId,
                                        ByteBuffer keyMetadata) {
    return new GenericDataFile(specId, content, filePath, format, partitionData, fileSizeInBytes, recordCount,
        columnSizes, valueCounts, nullValueCounts, nanValueCounts, lowerBounds, upperBounds, splitOffsets,
        equalityFieldIds, sortOrderId, keyMetadata);
  }

  public static DeleteFile createDeleteFile(int specId, FileContent content, String filePath, FileFormat format,
                                            PartitionData partitionData, long fileSizeInBytes, long recordCount,
                                            Map<Integer, Long> columnSizes, Map<Integer, Long> valueCounts,
                                            Map<Integer, Long> nullValueCounts, Map<Integer, Long> nanValueCounts,
                                            Map<Integer, ByteBuffer> lowerBounds, Map<Integer, ByteBuffer> upperBounds,
                                            List<Long> splitOffsets, int[] equalityFieldIds, Integer sortOrderId,
                                            ByteBuffer keyMetadata) {
    return new GenericDeleteFile(specId, content, filePath, format, partitionData, fileSizeInBytes, recordCount,
        columnSizes, valueCounts, nullValueCounts, nanValueCounts, lowerBounds, upperBounds, splitOffsets,
        equalityFieldIds, sortOrderId, keyMetadata);
  }

  public static List<byte[]> partitionDataToBytesMap(PartitionData partition) {
    return IntStream.of(partition.size())
        .mapToObj(i -> partitionValueToBytes(partition.get(i), i))
        .collect(Collectors.toList());
  }

  public static PartitionData bytesMapToPartitionData(List<byte[]> values, PartitionSpec spec) {
    PartitionData partitionData = new PartitionData(spec.partitionType());
    Class<?>[] classes = spec.javaClasses();
    IntStream.of(values.size()).forEach(i -> partitionData.set(i, bytesToPartitionValue(values.get(i), classes[i], i)));
    return partitionData;
  }

  private static byte[] partitionValueToBytes(Object value, int pos) {
    byte[] bytes;
    if (value == null) {
      bytes = null;
    } else if (value instanceof ByteBuffer) {
      ByteBuffer bb = (ByteBuffer) value;
      bytes = new byte[bb.remaining()];
      bb.put(bytes);
    } else if (value instanceof CharSequence) {
      bytes = value.toString().getBytes(StandardCharsets.UTF_8);
    } else if (value instanceof UUID) {
      ByteBuffer bb = ByteBuffer.allocate(16);
      bb.putLong(((UUID) value).getMostSignificantBits());
      bb.putLong(((UUID) value).getLeastSignificantBits());
      bytes = bb.array();
    } else if (value instanceof Boolean) {
      bytes = ByteBuffer.allocate(Integer.BYTES).putInt((Boolean) value ? 1 : 0).array();
    } else if (value instanceof Integer) {
      bytes = ByteBuffer.allocate(Integer.BYTES).putInt((Integer) value).array();
    } else if (value instanceof Long) {
      bytes = ByteBuffer.allocate(Long.BYTES).putLong((Long) value).array();
    } else if (value instanceof Float) {
      bytes = ByteBuffer.allocate(Float.BYTES).putFloat((Float) value).array();
    } else if (value instanceof Double) {
      bytes = ByteBuffer.allocate(Double.BYTES).putDouble((Double) value).array();
    } else if (value instanceof BigDecimal) {
      bytes = value.toString().getBytes(StandardCharsets.UTF_8);
    } else {
      throw new UnsupportedOperationException("Unsupported partition data value " + value + " at position" + pos);
    }

    return bytes;
  }

  private static Object bytesToPartitionValue(byte[] bytes, Class<?> javaClass, int pos) {
    Object value;
    if (bytes == null) {
      value = null;
    } else if (javaClass.equals(ByteBuffer.class)) {
      value = ByteBuffer.wrap(bytes);
    } else if (javaClass.equals(CharSequence.class)) {
      value = new String(bytes, StandardCharsets.UTF_8);
    } else if (javaClass.equals(UUID.class)) {
      value = UUID.nameUUIDFromBytes(bytes);
    } else if (javaClass.equals(Boolean.class)) {
      value = ByteBuffer.wrap(bytes).getInt() == 1;
    } else if (javaClass.equals(Integer.class)) {
      value = ByteBuffer.wrap(bytes).getInt();
    } else if (javaClass.equals(Long.class)) {
      value = ByteBuffer.wrap(bytes).getLong();
    } else if (javaClass.equals(Float.class)) {
      value = ByteBuffer.wrap(bytes).getFloat();
    } else if (javaClass.equals(Double.class)) {
      value = ByteBuffer.wrap(bytes).getDouble();
    } else if (javaClass.equals(BigDecimal.class)) {
      value = new BigDecimal(new String(bytes, StandardCharsets.UTF_8));
    } else {
      throw new UnsupportedOperationException("Unsupported partition data class " + javaClass + " at position" + pos);
    }

    return value;
  }

  public static <T> Map<T, ByteBuffer> bytesMapToByteBufferMap(Map<T, byte[]> map) {
    return map.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> ByteBuffer.wrap(e.getValue())));
  }

  public static <T> Map<T, byte[]> byteBufferMapToBytesMap(Map<T, ByteBuffer> map) {
    return map.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> {
      byte[] bytes = new byte[e.getValue().remaining()];
      e.getValue().put(bytes);
      return bytes;
    }));
  }
}
