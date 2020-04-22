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
import java.util.Locale;
import java.util.Map;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileStatus;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

public class SparkDataFile implements DataFile {

  private final Type lowerBoundsType;
  private final Type upperBoundsType;
  private final Type keyMetadataType;
  private final SparkStructLike wrappedPartition;
  private final int[] fieldPositions;
  private Row wrapped;

  public SparkDataFile(Types.StructType type, StructType sparkType) {
    this.lowerBoundsType = type.fieldType("lower_bounds");
    this.upperBoundsType = type.fieldType("upper_bounds");
    this.keyMetadataType = type.fieldType("key_metadata");
    this.wrappedPartition = new SparkStructLike(type.fieldType("partition").asStructType());
    this.fieldPositions = indexFields(type, sparkType);
  }

  public SparkDataFile wrap(Row row) {
    this.wrapped = row;
    if (wrappedPartition.size() > 0) {
      this.wrappedPartition.wrap(row.getAs(fieldPositions[2]));
    }
    return this;
  }

  @Override
  public FileStatus status() {
    return null;
  }

  @Override
  public Long snapshotId() {
    return null;
  }

  @Override
  public Long sequenceNumber() {
    return null;
  }

  @Override
  public CharSequence path() {
    return wrapped.getAs(fieldPositions[0]);
  }

  @Override
  public FileFormat format() {
    String formatAsString = wrapped.getString(fieldPositions[1]).toUpperCase(Locale.ROOT);
    return FileFormat.valueOf(formatAsString);
  }

  @Override
  public StructLike partition() {
    return wrappedPartition;
  }

  @Override
  public long recordCount() {
    return wrapped.getAs(fieldPositions[3]);
  }

  @Override
  public long fileSizeInBytes() {
    return wrapped.getAs(fieldPositions[4]);
  }

  @Override
  public Map<Integer, Long> columnSizes() {
    return wrapped.getJavaMap(fieldPositions[6]);
  }

  @Override
  public Map<Integer, Long> valueCounts() {
    return wrapped.getJavaMap(fieldPositions[7]);
  }

  @Override
  public Map<Integer, Long> nullValueCounts() {
    return wrapped.getJavaMap(fieldPositions[8]);
  }

  @Override
  public Map<Integer, ByteBuffer> lowerBounds() {
    return convert(lowerBoundsType, wrapped.getJavaMap(fieldPositions[9]));
  }

  @Override
  public Map<Integer, ByteBuffer> upperBounds() {
    return convert(upperBoundsType, wrapped.getJavaMap(fieldPositions[10]));
  }

  @Override
  public ByteBuffer keyMetadata() {
    return convert(keyMetadataType, wrapped.get(fieldPositions[11]));
  }

  @Override
  public DataFile copy() {
    throw new UnsupportedOperationException("Not implemented: copy");
  }

  @Override
  public DataFile copyWithoutStats() {
    throw new UnsupportedOperationException("Not implemented: copyWithoutStats");
  }

  @Override
  public List<Long> splitOffsets() {
    return wrapped.getList(fieldPositions[12]);
  }

  private int[] indexFields(Types.StructType type, StructType sparkType) {
    List<Types.NestedField> fields = type.fields();
    int[] positions = new int[fields.size()];
    for (int index = 0; index < fields.size(); index++) {
      Types.NestedField field = fields.get(index);
      positions[index] = fieldPosition(field.name(), sparkType);
    }
    return positions;
  }

  private int fieldPosition(String name, StructType sparkType) {
    try {
      return sparkType.fieldIndex(name);
    } catch (IllegalArgumentException e) {
      // the partition field is absent for unpartitioned tables
      if (name.equals("partition") && wrappedPartition.size() == 0) {
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
