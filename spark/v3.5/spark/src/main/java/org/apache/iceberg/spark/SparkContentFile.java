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
package org.apache.iceberg.spark;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructProjection;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

public abstract class SparkContentFile<F> implements ContentFile<F> {

  private static final FileContent[] FILE_CONTENT_VALUES = FileContent.values();

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
  private final Type lowerBoundsType;
  private final Type upperBoundsType;
  private final Type keyMetadataType;

  private final SparkStructLike wrappedPartition;
  private final StructLike projectedPartition;
  private Row wrapped;

  SparkContentFile(Types.StructType type, Types.StructType projectedType, StructType sparkType) {
    this.lowerBoundsType = type.fieldType(DataFile.LOWER_BOUNDS.name());
    this.upperBoundsType = type.fieldType(DataFile.UPPER_BOUNDS.name());
    this.keyMetadataType = type.fieldType(DataFile.KEY_METADATA.name());

    Types.StructType partitionType = type.fieldType(DataFile.PARTITION_NAME).asStructType();
    this.wrappedPartition = new SparkStructLike(partitionType);

    if (projectedType != null) {
      Types.StructType projectedPartitionType =
          projectedType.fieldType(DataFile.PARTITION_NAME).asStructType();
      StructProjection partitionProjection =
          StructProjection.create(partitionType, projectedPartitionType);
      this.projectedPartition = partitionProjection.wrap(wrappedPartition);
    } else {
      this.projectedPartition = wrappedPartition;
    }

    Map<String, Integer> positions = Maps.newHashMap();
    for (Types.NestedField field : type.fields()) {
      String fieldName = field.name();
      positions.put(fieldName, fieldPosition(fieldName, sparkType));
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
  }

  public F wrap(Row row) {
    this.wrapped = row;
    if (wrappedPartition.size() > 0) {
      wrappedPartition.wrap(row.getAs(partitionPosition));
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
    return FILE_CONTENT_VALUES[wrapped.getInt(fileContentPosition)];
  }

  @Override
  public CharSequence path() {
    return wrapped.getAs(filePathPosition);
  }

  @Override
  public FileFormat format() {
    return FileFormat.fromString(wrapped.getString(fileFormatPosition));
  }

  @Override
  public StructLike partition() {
    return projectedPartition;
  }

  @Override
  public long recordCount() {
    return wrapped.getAs(recordCountPosition);
  }

  @Override
  public long fileSizeInBytes() {
    return wrapped.getAs(fileSizeInBytesPosition);
  }

  @Override
  public Map<Integer, Long> columnSizes() {
    return wrapped.isNullAt(columnSizesPosition) ? null : wrapped.getJavaMap(columnSizesPosition);
  }

  @Override
  public Map<Integer, Long> valueCounts() {
    return wrapped.isNullAt(valueCountsPosition) ? null : wrapped.getJavaMap(valueCountsPosition);
  }

  @Override
  public Map<Integer, Long> nullValueCounts() {
    if (wrapped.isNullAt(nullValueCountsPosition)) {
      return null;
    }
    return wrapped.getJavaMap(nullValueCountsPosition);
  }

  @Override
  public Map<Integer, Long> nanValueCounts() {
    if (wrapped.isNullAt(nanValueCountsPosition)) {
      return null;
    }
    return wrapped.getJavaMap(nanValueCountsPosition);
  }

  @Override
  public Map<Integer, ByteBuffer> lowerBounds() {
    Map<?, ?> lowerBounds =
        wrapped.isNullAt(lowerBoundsPosition) ? null : wrapped.getJavaMap(lowerBoundsPosition);
    return convert(lowerBoundsType, lowerBounds);
  }

  @Override
  public Map<Integer, ByteBuffer> upperBounds() {
    Map<?, ?> upperBounds =
        wrapped.isNullAt(upperBoundsPosition) ? null : wrapped.getJavaMap(upperBoundsPosition);
    return convert(upperBoundsType, upperBounds);
  }

  @Override
  public ByteBuffer keyMetadata() {
    return convert(keyMetadataType, wrapped.get(keyMetadataPosition));
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
    return wrapped.isNullAt(splitOffsetsPosition) ? null : wrapped.getList(splitOffsetsPosition);
  }

  @Override
  public Integer sortOrderId() {
    return wrapped.getAs(sortOrderIdPosition);
  }

  @Override
  public List<Integer> equalityFieldIds() {
    return wrapped.isNullAt(equalityIdsPosition) ? null : wrapped.getList(equalityIdsPosition);
  }

  private int fieldPosition(String name, StructType sparkType) {
    try {
      return sparkType.fieldIndex(name);
    } catch (IllegalArgumentException e) {
      // the partition field is absent for unpartitioned tables
      if (name.equals(DataFile.PARTITION_NAME) && wrappedPartition.size() == 0) {
        return -1;
      }
      throw e;
    }
  }

  @SuppressWarnings("unchecked")
  private <T> T convert(Type valueType, Object value) {
    return (T) SparkValueConverter.convert(valueType, value);
  }
}
