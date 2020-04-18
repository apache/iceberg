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
import org.apache.iceberg.StructLike;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Row;

public class SparkDataFile implements DataFile {

  private final Type lowerBoundsType;
  private final Type upperBoundsType;
  private final Type keyMetadataType;
  private final int fieldShift;
  private final SparkStructLike wrappedPartition;
  private Row wrapped;

  public SparkDataFile(Types.StructType type) {
    this.lowerBoundsType = type.fieldType("lower_bounds");
    this.upperBoundsType = type.fieldType("upper_bounds");
    this.keyMetadataType = type.fieldType("key_metadata");
    this.wrappedPartition = new SparkStructLike(type.fieldType("partition").asStructType());
    // the partition field is absent for unpartitioned tables
    this.fieldShift = wrappedPartition.size() != 0 ? 1 : 0;
  }

  public SparkDataFile wrap(Row row) {
    this.wrapped = row;
    if (wrappedPartition.size() > 0) {
      this.wrappedPartition.wrap(row.getAs(2));
    }
    return this;
  }

  @Override
  public CharSequence path() {
    return wrapped.getAs(0);
  }

  @Override
  public FileFormat format() {
    String formatAsString = wrapped.getString(1).toUpperCase(Locale.ROOT);
    return FileFormat.valueOf(formatAsString);
  }

  @Override
  public StructLike partition() {
    return wrappedPartition;
  }

  @Override
  public long recordCount() {
    return wrapped.getAs(fieldShift + 2);
  }

  @Override
  public long fileSizeInBytes() {
    return wrapped.getAs(fieldShift + 3);
  }

  @Override
  public Map<Integer, Long> columnSizes() {
    return wrapped.getJavaMap(fieldShift + 5);
  }

  @Override
  public Map<Integer, Long> valueCounts() {
    return wrapped.getJavaMap(fieldShift + 6);
  }

  @Override
  public Map<Integer, Long> nullValueCounts() {
    return wrapped.getJavaMap(fieldShift + 7);
  }

  @Override
  public Map<Integer, ByteBuffer> lowerBounds() {
    return convert(lowerBoundsType, wrapped.getJavaMap(fieldShift + 8));
  }

  @Override
  public Map<Integer, ByteBuffer> upperBounds() {
    return convert(upperBoundsType, wrapped.getJavaMap(fieldShift + 9));
  }

  @Override
  public ByteBuffer keyMetadata() {
    return convert(keyMetadataType, wrapped.get(fieldShift + 10));
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
    return wrapped.getList(fieldShift + 11);
  }

  @SuppressWarnings("unchecked")
  private <T> T convert(Type valueType, Object value) {
    return (T) SparkValueConverter.convert(valueType, value);
  }
}
