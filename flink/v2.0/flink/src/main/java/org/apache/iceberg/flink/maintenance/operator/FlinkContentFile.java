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
package org.apache.iceberg.flink.maintenance.operator;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.flink.RowDataWrapper;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructProjection;

abstract class FlinkContentFile<F> implements ContentFile<F> {

  private final int fileContentPosition;
  private final int filePathPosition;
  private final int fileFormatPosition;
  private final int partitionPosition;
  private final int recordCountPosition;
  private final int fileSizeInBytesPosition;
  private final int columnSizesPosition;
  private final int valueCountsPosition;
  private final int nullValueCountsPosition;
  private final int nanValueCountsPosition;
  private final int lowerBoundsPosition;
  private final int upperBoundsPosition;
  private final int keyMetadataPosition;
  private final int splitOffsetsPosition;
  private final int sortOrderIdPosition;
  private final int equalityIdsPosition;
  private final int dataFileFieldCount;

  private final RowDataWrapper wrappedPartition;
  private final StructLike projectedPartition;
  private RowData wrapped;

  FlinkContentFile(Types.StructType type, Types.StructType projectedType, RowType flinkType) {
    Map<String, Integer> positions = Maps.newHashMap();
    for (Types.NestedField field : type.fields()) {
      String fieldName = field.name();
      positions.put(fieldName, fieldPosition(fieldName, flinkType));
    }

    this.fileContentPosition = positions.get(DataFile.CONTENT.name());
    this.filePathPosition = positions.get(DataFile.FILE_PATH.name());
    this.fileFormatPosition = positions.get(DataFile.FILE_FORMAT.name());
    this.partitionPosition = positions.get(DataFile.PARTITION_NAME);
    this.recordCountPosition = positions.get(DataFile.RECORD_COUNT.name());
    this.fileSizeInBytesPosition = positions.get(DataFile.FILE_SIZE.name());
    this.columnSizesPosition = positions.get(DataFile.COLUMN_SIZES.name());
    this.valueCountsPosition = positions.get(DataFile.VALUE_COUNTS.name());
    this.nullValueCountsPosition = positions.get(DataFile.NULL_VALUE_COUNTS.name());
    this.nanValueCountsPosition = positions.get(DataFile.NAN_VALUE_COUNTS.name());
    this.lowerBoundsPosition = positions.get(DataFile.LOWER_BOUNDS.name());
    this.upperBoundsPosition = positions.get(DataFile.UPPER_BOUNDS.name());
    this.keyMetadataPosition = positions.get(DataFile.KEY_METADATA.name());
    this.splitOffsetsPosition = positions.get(DataFile.SPLIT_OFFSETS.name());
    this.sortOrderIdPosition = positions.get(DataFile.SORT_ORDER_ID.name());
    this.equalityIdsPosition = positions.get(DataFile.EQUALITY_IDS.name());
    this.dataFileFieldCount = flinkType.getFieldCount();

    // Initialize the projected partition and the underlying wrapper objects
    Types.StructType partitionType = type.fieldType(DataFile.PARTITION_NAME).asStructType();
    RowType partitionRowType =
        positions.get(DataFile.PARTITION_NAME) != -1
            ? (RowType) flinkType.getTypeAt(positions.get(DataFile.PARTITION_NAME))
            : new RowType(ImmutableList.of());
    this.wrappedPartition = new RowDataWrapper(partitionRowType, partitionType);

    if (projectedType != null) {
      Types.StructType projectedPartitionType =
          projectedType.fieldType(DataFile.PARTITION_NAME).asStructType();
      StructProjection partitionProjection =
          StructProjection.create(partitionType, projectedPartitionType);
      this.projectedPartition = partitionProjection.wrap(wrappedPartition);
    } else {
      this.projectedPartition = wrappedPartition;
    }
  }

  public F wrap(RowData row) {
    this.wrapped = row;
    if (wrappedPartition.size() > 0) {
      wrappedPartition.wrap(row.getRow(partitionPosition, dataFileFieldCount));
    }

    return asFile();
  }

  protected abstract F asFile();

  @Override
  public Long pos() {
    return null;
  }

  @Override
  public int specId() {
    return -1;
  }

  @Override
  public FileContent content() {
    if (wrapped.isNullAt(fileContentPosition)) {
      return null;
    }

    return FileContent.values()[wrapped.getInt(fileContentPosition)];
  }

  @Override
  public CharSequence path() {
    return wrapped.getString(filePathPosition).toString();
  }

  @Override
  public FileFormat format() {
    return FileFormat.fromString(wrapped.getString(fileFormatPosition).toString());
  }

  @Override
  public StructLike partition() {
    return projectedPartition;
  }

  @Override
  public long recordCount() {
    return wrapped.getLong(recordCountPosition);
  }

  @Override
  public long fileSizeInBytes() {
    return wrapped.getLong(fileSizeInBytesPosition);
  }

  @Override
  public Map<Integer, Long> columnSizes() {
    return wrapped.isNullAt(columnSizesPosition)
        ? null
        : convert(wrapped.getMap(columnSizesPosition));
  }

  @Override
  public Map<Integer, Long> valueCounts() {
    return wrapped.isNullAt(valueCountsPosition)
        ? null
        : convert(wrapped.getMap(valueCountsPosition));
  }

  @Override
  public Map<Integer, Long> nullValueCounts() {
    if (wrapped.isNullAt(nullValueCountsPosition)) {
      return null;
    }

    return convert(wrapped.getMap(nullValueCountsPosition));
  }

  @Override
  public Map<Integer, Long> nanValueCounts() {
    if (wrapped.isNullAt(nanValueCountsPosition)) {
      return null;
    }

    return convert(wrapped.getMap(nanValueCountsPosition));
  }

  @Override
  public Map<Integer, ByteBuffer> lowerBounds() {
    if (wrapped.isNullAt(lowerBoundsPosition)) {
      return null;
    }

    return convertToByteBufferMap(wrapped.getMap(lowerBoundsPosition));
  }

  @Override
  public Map<Integer, ByteBuffer> upperBounds() {
    if (wrapped.isNullAt(upperBoundsPosition)) {
      return null;
    }

    return convertToByteBufferMap(wrapped.getMap(upperBoundsPosition));
  }

  @Override
  public ByteBuffer keyMetadata() {
    if (wrapped.isNullAt(keyMetadataPosition)) {
      return null;
    }

    return ByteBuffer.wrap(wrapped.getBinary(keyMetadataPosition));
  }

  @Override
  public F copy() {
    throw new UnsupportedOperationException("Not implemented: copy");
  }

  @Override
  public F copyWithoutStats() {
    throw new UnsupportedOperationException("Not implemented: copyWithoutStats");
  }

  @Override
  public List<Long> splitOffsets() {
    return wrapped.isNullAt(splitOffsetsPosition)
        ? null
        : Arrays.stream(wrapped.getArray(splitOffsetsPosition).toLongArray())
            .boxed()
            .collect(Collectors.toList());
  }

  @Override
  public Integer sortOrderId() {
    if (wrapped.isNullAt(sortOrderIdPosition)) {
      return null;
    }

    return wrapped.getInt(sortOrderIdPosition);
  }

  @Override
  public List<Integer> equalityFieldIds() {
    return wrapped.isNullAt(equalityIdsPosition)
        ? null
        : Arrays.stream(wrapped.getArray(equalityIdsPosition).toIntArray())
            .boxed()
            .collect(Collectors.toList());
  }

  private int fieldPosition(String name, RowType flinkType) {
    try {
      return flinkType.getFieldIndex(name);
    } catch (IllegalArgumentException e) {
      // the partition field is absent for unpartitioned tables
      if (name.equals(DataFile.PARTITION_NAME) && wrappedPartition.size() == 0) {
        return -1;
      }

      throw e;
    }
  }

  private Map<Integer, Long> convert(MapData mapData) {
    int[] keys = mapData.keyArray().toIntArray();
    long[] values = mapData.valueArray().toLongArray();
    Map<Integer, Long> result = Maps.newHashMapWithExpectedSize(keys.length);
    for (int i = 0; i < keys.length; ++i) {
      result.put(keys[i], values[i]);
    }

    return result;
  }

  private Map<Integer, ByteBuffer> convertToByteBufferMap(MapData mapData) {
    int[] keys = mapData.keyArray().toIntArray();
    ArrayData values = mapData.valueArray();
    Map<Integer, ByteBuffer> result = Maps.newHashMapWithExpectedSize(keys.length);
    for (int i = 0; i < keys.length; ++i) {
      result.put(keys[i], ByteBuffer.wrap(values.getBinary(i)));
    }

    return result;
  }
}
