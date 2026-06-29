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

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.util.Objects;
import org.apache.iceberg.avro.SupportsIndexProjection;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ByteBuffers;

class BaseFieldStats<T> extends SupportsIndexProjection implements FieldStats<T> {
  private static final int[] IDENTITY_MAPPING = identityMapping();
  private final int fieldId;
  private final Type type;
  private final T lowerBound;
  private final T upperBound;
  private final boolean tightBounds;
  private final Long valueCount;
  private final Long nullValueCount;
  private final Long nanValueCount;
  private final Integer avgValueSizeInBytes;

  private BaseFieldStats(
      int fieldId,
      int[] fromProjectionPos,
      Type type,
      T lowerBound,
      T upperBound,
      boolean tightBounds,
      Long valueCount,
      Long nullValueCount,
      Long nanValueCount,
      Integer avgValueSizeInBytes) {
    super(fromProjectionPos != null ? fromProjectionPos : IDENTITY_MAPPING);
    this.fieldId = fieldId;
    this.type = type;
    this.lowerBound = lowerBound;
    this.upperBound = upperBound;
    this.tightBounds = tightBounds;
    this.valueCount = valueCount;
    this.nullValueCount = nullValueCount;
    this.nanValueCount = nanValueCount;
    this.avgValueSizeInBytes = avgValueSizeInBytes;
  }

  private static int[] identityMapping() {
    int numStats = FieldStatistic.values().length;
    int[] mapping = new int[numStats];
    for (int i = 0; i < numStats; i++) {
      mapping[i] = i;
    }

    return mapping;
  }

  /**
   * Computes a position mapping from the column-specific stats struct to the full stats struct.
   * Each entry maps a projected position to its base position (0-based) using the field ID offsets
   * from the column's base stats field ID.
   */
  private static int[] projectionMapping(Types.StructType statsStruct, int dataFieldId) {
    if (statsStruct == null) {
      return null;
    }

    int baseStatsFieldId = StatsUtil.statsFieldIdForField(dataFieldId);
    int[] mapping = new int[statsStruct.fields().size()];
    for (int i = 0; i < mapping.length; i++) {
      // offset is 1-based (matching FieldStatistic.offset()), position is 0-based
      mapping[i] = statsStruct.fields().get(i).fieldId() - baseStatsFieldId - 1;
    }

    return mapping;
  }

  @Override
  public int fieldId() {
    return fieldId;
  }

  @Override
  public Type type() {
    return type;
  }

  @Override
  public Long valueCount() {
    return valueCount;
  }

  @Override
  public Long nullValueCount() {
    return nullValueCount;
  }

  @Override
  public Long nanValueCount() {
    return nanValueCount;
  }

  @Override
  public Integer avgValueSizeInBytes() {
    return avgValueSizeInBytes;
  }

  @SuppressWarnings("unchecked")
  private static <T> T serializableBound(T bound) {
    if (bound instanceof CharBuffer) {
      // CharBuffer is not serializable, use String instead
      return (T) bound.toString();
    } else if (bound instanceof ByteBuffer) {
      // ByteBuffer is not serializable, use byte[] instead
      return (T) ByteBuffers.toByteArray((ByteBuffer) bound);
    }

    return bound;
  }

  @SuppressWarnings("unchecked")
  @Override
  public T lowerBound() {
    if (null != type
        && type.typeId().javaClass().equals(ByteBuffer.class)
        && lowerBound instanceof byte[]) {
      // for serializability we store binary types as byte[] and must convert back to
      // ByteBuffer
      return (T) ByteBuffer.wrap((byte[]) lowerBound);
    } else {
      return lowerBound;
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public T upperBound() {
    if (null != type
        && type.typeId().javaClass().equals(ByteBuffer.class)
        && upperBound instanceof byte[]) {
      // for serializability we store binary types as byte[] and must convert back to
      // ByteBuffer
      return (T) ByteBuffer.wrap((byte[]) upperBound);
    } else {
      return upperBound;
    }
  }

  @Override
  public boolean tightBounds() {
    return tightBounds;
  }

  @Override
  protected <X> X internalGet(int pos, Class<X> javaClass) {
    return switch (FieldStatistic.fromPosition(pos)) {
      case LOWER_BOUND -> javaClass.cast(lowerBound());
      case UPPER_BOUND -> javaClass.cast(upperBound());
      case TIGHT_BOUNDS -> javaClass.cast(tightBounds);
      case VALUE_COUNT -> javaClass.cast(valueCount);
      case NULL_VALUE_COUNT -> javaClass.cast(nullValueCount);
      case NAN_VALUE_COUNT -> javaClass.cast(nanValueCount);
      case AVG_VALUE_SIZE_IN_BYTES -> javaClass.cast(avgValueSizeInBytes);
      default -> throw new UnsupportedOperationException("Unknown field ordinal: " + pos);
    };
  }

  @Override
  protected <X> void internalSet(int pos, X value) {
    throw new UnsupportedOperationException("set() not supported");
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("fieldId", fieldId)
        .add("type", type)
        .add("lowerBound", lowerBound)
        .add("upperBound", upperBound)
        .add("tightBounds", tightBounds)
        .add("valueCount", valueCount)
        .add("nullValueCount", nullValueCount)
        .add("nanValueCount", nanValueCount)
        .add("avgValueSizeInBytes", avgValueSizeInBytes)
        .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof BaseFieldStats)) {
      return false;
    }

    BaseFieldStats<?> that = (BaseFieldStats<?>) o;
    return fieldId == that.fieldId
        && tightBounds == that.tightBounds
        && Objects.equals(type, that.type)
        && Objects.deepEquals(lowerBound, that.lowerBound)
        && Objects.deepEquals(upperBound, that.upperBound)
        && Objects.equals(valueCount, that.valueCount)
        && Objects.equals(nullValueCount, that.nullValueCount)
        && Objects.equals(nanValueCount, that.nanValueCount)
        && Objects.equals(avgValueSizeInBytes, that.avgValueSizeInBytes);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        fieldId,
        type,
        lowerBound,
        upperBound,
        tightBounds,
        valueCount,
        nullValueCount,
        nanValueCount,
        avgValueSizeInBytes);
  }

  public static <X> Builder<X> builder() {
    return new Builder<>();
  }

  public static <X> Builder<X> buildFrom(FieldStats<X> value) {
    Preconditions.checkArgument(null != value, "Invalid column stats: null");
    return BaseFieldStats.<X>builder()
        .type(value.type())
        .fieldId(value.fieldId())
        .lowerBound(value.lowerBound())
        .upperBound(value.upperBound())
        .tightBounds(value.tightBounds())
        .valueCount(value.valueCount())
        .nullValueCount(value.nullValueCount())
        .nanValueCount(value.nanValueCount())
        .avgValueSizeInBytes(value.avgValueSizeInBytes());
  }

  public static class Builder<T> {
    private int fieldId;
    private int[] fromProjectionPos;
    private Type type;
    private T lowerBound;
    private T upperBound;
    private boolean tightBounds;
    private Long valueCount;
    private Long nullValueCount;
    private Long nanValueCount;
    private Integer avgValueSizeInBytes;

    private Builder() {}

    public Builder<T> statsStruct(Types.StructType statsStruct) {
      this.fromProjectionPos = projectionMapping(statsStruct, fieldId);
      return this;
    }

    public Builder<T> type(Type newType) {
      this.type = newType;
      return this;
    }

    public Builder<T> valueCount(Long newValueCount) {
      this.valueCount = newValueCount;
      return this;
    }

    public Builder<T> nullValueCount(Long newNullValueCount) {
      this.nullValueCount = newNullValueCount;
      return this;
    }

    public Builder<T> nanValueCount(Long newNanValueCount) {
      this.nanValueCount = newNanValueCount;
      return this;
    }

    public Builder<T> avgValueSizeInBytes(Integer newAvgValueSizeInBytes) {
      this.avgValueSizeInBytes = newAvgValueSizeInBytes;
      return this;
    }

    public Builder<T> lowerBound(T newLowerBound) {
      this.lowerBound = serializableBound(newLowerBound);
      return this;
    }

    public Builder<T> upperBound(T newUpperBound) {
      this.upperBound = serializableBound(newUpperBound);
      return this;
    }

    public Builder<T> fieldId(int newFieldId) {
      this.fieldId = newFieldId;
      return this;
    }

    public Builder<T> tightBounds(boolean newTightBounds) {
      this.tightBounds = newTightBounds;
      return this;
    }

    public Builder<T> tightBounds() {
      this.tightBounds = true;
      return this;
    }

    public BaseFieldStats<T> build() {
      if (null != lowerBound) {
        Preconditions.checkArgument(
            null != type, "Invalid type (required when lower bound is set): null");
        Preconditions.checkArgument(
            type.typeId().javaClass().isInstance(lowerBound)
                || (type.typeId().javaClass().equals(ByteBuffer.class)
                    && lowerBound instanceof byte[]),
            "Invalid lower bound type, expected a subtype of %s: %s",
            type.typeId().javaClass().getName(),
            lowerBound.getClass().getName());
      }

      if (null != upperBound) {
        Preconditions.checkArgument(
            null != type, "Invalid type (required when lower bound is set): null");
        Preconditions.checkArgument(
            type.typeId().javaClass().isInstance(upperBound)
                || (type.typeId().javaClass().equals(ByteBuffer.class)
                    && upperBound instanceof byte[]),
            "Invalid upper bound type, expected a subtype of %s: %s",
            type.typeId().javaClass().getName(),
            upperBound.getClass().getName());
      }

      return new BaseFieldStats<>(
          fieldId,
          fromProjectionPos,
          type,
          lowerBound,
          upperBound,
          tightBounds,
          valueCount,
          nullValueCount,
          nanValueCount,
          avgValueSizeInBytes);
    }
  }
}
