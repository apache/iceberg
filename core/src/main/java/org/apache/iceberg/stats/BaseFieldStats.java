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
package org.apache.iceberg.stats;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.util.Objects;
import org.apache.iceberg.avro.SupportsIndexProjection;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ByteBuffers;

public class BaseFieldStats<T> extends SupportsIndexProjection implements FieldStats<T> {
  private static final int[] IDENTITY_MAPPING = identityMapping();
  private final int fieldId;
  private final Type type;
  private final Long valueCount;
  private final Long nullValueCount;
  private final Long nanValueCount;
  private final Integer avgValueSize;
  private final Integer maxValueSize;
  private final T lowerBound;
  private final T upperBound;
  private final boolean hasExactBounds;

  private BaseFieldStats(
      int fieldId,
      int[] fromProjectionPos,
      Type type,
      Long valueCount,
      Long nullValueCount,
      Long nanValueCount,
      Integer avgValueSize,
      Integer maxValueSize,
      T lowerBound,
      T upperBound,
      boolean hasExactBounds) {
    super(fromProjectionPos != null ? fromProjectionPos : IDENTITY_MAPPING);
    this.fieldId = fieldId;
    this.type = type;
    this.valueCount = valueCount;
    this.nullValueCount = nullValueCount;
    this.nanValueCount = nanValueCount;
    this.avgValueSize = avgValueSize;
    this.maxValueSize = maxValueSize;
    this.lowerBound = lowerBound;
    this.upperBound = upperBound;
    this.hasExactBounds = hasExactBounds;
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
   * Computes a position mapping from the column-specific stats struct to the full 8-field struct.
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
  public Integer avgValueSize() {
    return avgValueSize;
  }

  @Override
  public Integer maxValueSize() {
    return maxValueSize;
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
  public boolean hasExactBounds() {
    return hasExactBounds;
  }

  @Override
  protected <X> X internalGet(int pos, Class<X> javaClass) {
    return switch (FieldStatistic.fromPosition(pos)) {
      case VALUE_COUNT -> javaClass.cast(valueCount);
      case NULL_VALUE_COUNT -> javaClass.cast(nullValueCount);
      case NAN_VALUE_COUNT -> javaClass.cast(nanValueCount);
      case AVG_VALUE_SIZE -> javaClass.cast(avgValueSize);
      case MAX_VALUE_SIZE -> javaClass.cast(maxValueSize);
      case LOWER_BOUND -> javaClass.cast(lowerBound());
      case UPPER_BOUND -> javaClass.cast(upperBound());
      case EXACT_BOUNDS -> javaClass.cast(hasExactBounds);
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
        .add("valueCount", valueCount)
        .add("nullValueCount", nullValueCount)
        .add("nanValueCount", nanValueCount)
        .add("avgValueSize", avgValueSize)
        .add("maxValueSize", maxValueSize)
        .add("lowerBound", lowerBound)
        .add("upperBound", upperBound)
        .add("hasExactBounds", hasExactBounds)
        .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof BaseFieldStats)) {
      return false;
    }

    BaseFieldStats<?> that = (BaseFieldStats<?>) o;
    return fieldId == that.fieldId
        && Objects.equals(type, that.type)
        && Objects.equals(valueCount, that.valueCount)
        && Objects.equals(nullValueCount, that.nullValueCount)
        && Objects.equals(nanValueCount, that.nanValueCount)
        && Objects.equals(avgValueSize, that.avgValueSize)
        && Objects.equals(maxValueSize, that.maxValueSize)
        && Objects.deepEquals(lowerBound, that.lowerBound)
        && Objects.deepEquals(upperBound, that.upperBound)
        && hasExactBounds == that.hasExactBounds;
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        fieldId,
        type,
        valueCount,
        nullValueCount,
        nanValueCount,
        avgValueSize,
        maxValueSize,
        lowerBound,
        upperBound,
        hasExactBounds);
  }

  public static <X> Builder<X> builder() {
    return new Builder<>();
  }

  public static <X> Builder<X> buildFrom(FieldStats<X> value) {
    Preconditions.checkArgument(null != value, "Invalid column stats: null");
    return BaseFieldStats.<X>builder()
        .type(value.type())
        .fieldId(value.fieldId())
        .valueCount(value.valueCount())
        .nullValueCount(value.nullValueCount())
        .nanValueCount(value.nanValueCount())
        .avgValueSize(value.avgValueSize())
        .maxValueSize(value.maxValueSize())
        .lowerBound(value.lowerBound())
        .upperBound(value.upperBound())
        .hasExactBounds(value.hasExactBounds());
  }

  public static class Builder<T> {
    private int fieldId;
    private int[] fromProjectionPos;
    private Type type;
    private Long valueCount;
    private Long nullValueCount;
    private Long nanValueCount;
    private Integer avgValueSize;
    private Integer maxValueSize;
    private T lowerBound;
    private T upperBound;
    private boolean hasExactBounds;

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

    public Builder<T> avgValueSize(Integer newAvgValueSize) {
      this.avgValueSize = newAvgValueSize;
      return this;
    }

    public Builder<T> maxValueSize(Integer newMaxValueSize) {
      this.maxValueSize = newMaxValueSize;
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

    public Builder<T> hasExactBounds(boolean newHasExactBounds) {
      this.hasExactBounds = newHasExactBounds;
      return this;
    }

    public Builder<T> hasExactBounds() {
      this.hasExactBounds = true;
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
          valueCount,
          nullValueCount,
          nanValueCount,
          avgValueSize,
          maxValueSize,
          lowerBound,
          upperBound,
          hasExactBounds);
    }
  }
}
