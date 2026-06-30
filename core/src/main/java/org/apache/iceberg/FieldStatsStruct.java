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

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ByteBuffers;

class FieldStatsStruct<T> implements FieldStats<T>, StructLike, Serializable {
  private final Types.StructType struct;
  private final Type boundType;
  private final int[] posToOffset;
  private final int fieldId;

  private Object lowerBound = null;
  private Object upperBound = null;
  private boolean tightBounds = false;
  private Long valueCount = null;
  private Long nullValueCount = null;
  private Long nanValueCount = null;
  private Integer avgValueSize = null;

  FieldStatsStruct(Types.StructType struct) {
    this.struct = struct;
    this.posToOffset = posToOffset(struct);
    this.fieldId = StatsUtil.toFieldId(struct.fields().get(0).fieldId());
    this.boundType = struct.fieldType("lower_bound");
  }

  FieldStatsStruct(
      Types.StructType struct,
      T lowerBound,
      T upperBound,
      boolean tightBounds,
      long valueCount,
      long nullValueCount,
      long nanValueCount,
      Integer avgValueSize) {
    this(struct);
    setLowerBound(lowerBound);
    setUpperBound(upperBound);
    this.tightBounds = tightBounds;
    this.valueCount = valueCount;
    this.nullValueCount = nullValueCount;
    this.nanValueCount = nanValueCount;
    this.avgValueSize = avgValueSize;
  }

  private FieldStatsStruct(FieldStatsStruct<T> toCopy) {
    this(toCopy.struct);
    // bounds are stored using the internal representation, which is a byte array for binary types
    this.lowerBound = toCopy.lowerBound instanceof byte[] ? copyOf((byte[]) toCopy.lowerBound) : toCopy.lowerBound;
    this.upperBound = toCopy.upperBound instanceof byte[] ? copyOf((byte[]) toCopy.upperBound) : toCopy.upperBound;
    this.tightBounds = toCopy.tightBounds;
    this.valueCount = toCopy.valueCount;
    this.nullValueCount = toCopy.nullValueCount;
    this.nanValueCount = toCopy.nanValueCount;
    this.avgValueSize = toCopy.avgValueSize;
  }

  void fromFieldMetrics(FieldMetrics<T> fieldMetrics) {
    Preconditions.checkArgument(
        fieldMetrics.id() == fieldId,
        "Cannot store stats for field ID: %s (expected %s)",
        fieldMetrics.id(),
        fieldId);

    setLowerBound(fieldMetrics.lowerBound());
    setUpperBound(fieldMetrics.upperBound());
    this.tightBounds = false;
    this.valueCount = fieldMetrics.valueCount();
    this.nullValueCount = fieldMetrics.nullValueCount() < 0 ? null : fieldMetrics.nullValueCount();
    this.nanValueCount = fieldMetrics.nanValueCount() < 0 ? null : fieldMetrics.nanValueCount();
    this.avgValueSize = null;
  }

  private boolean isBinary() {
    return boundType != null
        && (boundType.typeId() == Type.TypeID.FIXED || boundType.typeId() == Type.TypeID.BINARY);
  }

  private void setLowerBound(Object lowerBound) {
    this.lowerBound = isBinary() ? ByteBuffers.toByteArray((ByteBuffer) lowerBound) : lowerBound;
  }

  private void setUpperBound(Object upperBound) {
    this.upperBound = isBinary() ? ByteBuffers.toByteArray((ByteBuffer) upperBound) : upperBound;
  }

  @Override
  public int fieldId() {
    return fieldId;
  }

  @Override
  public Types.StructType type() {
    return struct;
  }

  @Override
  @SuppressWarnings("unchecked")
  public T lowerBound() {
    return (T)
        (lowerBound != null && isBinary() ? ByteBuffer.wrap((byte[]) lowerBound) : lowerBound);
  }

  @Override
  @SuppressWarnings("unchecked")
  public T upperBound() {
    return (T)
        (upperBound != null && isBinary() ? ByteBuffer.wrap((byte[]) upperBound) : upperBound);
  }

  @Override
  public boolean tightBounds() {
    return tightBounds;
  }

  @Override
  public long valueCount() {
    return valueCount;
  }

  @Override
  public long nullValueCount() {
    return nullValueCount;
  }

  @Override
  public long nanValueCount() {
    return nanValueCount;
  }

  @Override
  public Integer avgValueSizeInBytes() {
    return avgValueSize;
  }

  @Override
  public int size() {
    return struct.fields().size();
  }

  private Object getOffset(int offset) {
    return switch (offset) {
      case StatsUtil.LOWER_BOUND_OFFSET -> lowerBound();
      case StatsUtil.UPPER_BOUND_OFFSET -> upperBound();
      case StatsUtil.TIGHT_BOUNDS_OFFSET -> tightBounds;
      case StatsUtil.VALUE_COUNT_OFFSET -> valueCount;
      case StatsUtil.NULL_VALUE_COUNT_OFFSET -> nullValueCount;
      case StatsUtil.NAN_VALUE_COUNT_OFFSET -> nanValueCount;
      case StatsUtil.AVG_VALUE_SIZE_OFFSET -> avgValueSize;
      default -> throw new UnsupportedOperationException("Unsupported stats offset: " + offset);
    };
  }

  @Override
  public <C> C get(int pos, Class<C> javaClass) {
    return javaClass.cast(getOffset(posToOffset[pos]));
  }

  private void setOffset(int offset, Object value) {
    switch (offset) {
      case StatsUtil.LOWER_BOUND_OFFSET -> setLowerBound(value);
      case StatsUtil.UPPER_BOUND_OFFSET -> setUpperBound(value);
      case StatsUtil.TIGHT_BOUNDS_OFFSET -> this.tightBounds = (Boolean) value;
      case StatsUtil.VALUE_COUNT_OFFSET -> this.valueCount = (Long) value;
      case StatsUtil.NULL_VALUE_COUNT_OFFSET -> this.nullValueCount = (Long) value;
      case StatsUtil.NAN_VALUE_COUNT_OFFSET -> this.nanValueCount = (Long) value;
      case StatsUtil.AVG_VALUE_SIZE_OFFSET -> this.avgValueSize = (Integer) value;
      default -> throw new UnsupportedOperationException("Unsupported stats offset: " + offset);
    }
  }

  @Override
  public <C> void set(int pos, C value) {
    setOffset(posToOffset[pos], value);
  }

  @Override
  public FieldStatsStruct<T> copy() {
    return new FieldStatsStruct<>(this);
  }

  private static int[] posToOffset(Types.StructType struct) {
    List<Types.NestedField> fields = struct.fields();
    int[] posToOffset = new int[fields.size()];
    for (int i = 0; i < posToOffset.length; i += 1) {
      posToOffset[i] = StatsUtil.statOffset(fields.get(i).fieldId());
    }

    return posToOffset;
  }

  private static byte[] copyOf(byte[] array) {
    return Arrays.copyOf(array, array.length);
  }
}
