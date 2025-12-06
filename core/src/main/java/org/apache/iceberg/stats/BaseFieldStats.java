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

import java.io.Serializable;
import java.util.Objects;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Type;

public class BaseFieldStats<T> implements FieldStats<T>, Serializable {
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
      Type type,
      Long valueCount,
      Long nullValueCount,
      Long nanValueCount,
      Integer avgValueSize,
      Integer maxValueSize,
      T lowerBound,
      T upperBound,
      boolean hasExactBounds) {
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

  @Override
  public T lowerBound() {
    return lowerBound;
  }

  @Override
  public T upperBound() {
    return upperBound;
  }

  @Override
  public boolean hasExactBounds() {
    return hasExactBounds;
  }

  @Override
  public int size() {
    return 7;
  }

  @Override
  public <X> X get(int pos, Class<X> javaClass) {
    switch (FieldStatistic.fromOffset(pos)) {
      case VALUE_COUNT:
        return javaClass.cast(valueCount);
      case NULL_VALUE_COUNT:
        return javaClass.cast(nullValueCount);
      case NAN_VALUE_COUNT:
        return javaClass.cast(nanValueCount);
      case AVG_VALUE_SIZE:
        return javaClass.cast(avgValueSize);
      case MAX_VALUE_SIZE:
        return javaClass.cast(maxValueSize);
      case LOWER_BOUND:
        return javaClass.cast(lowerBound);
      case UPPER_BOUND:
        return javaClass.cast(upperBound);
      case EXACT_BOUNDS:
        return javaClass.cast(hasExactBounds);
      default:
        throw new UnsupportedOperationException("Unknown field ordinal: " + pos);
    }
  }

  @Override
  public void set(int pos, Object value) {
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
        && Objects.equals(lowerBound, that.lowerBound)
        && Objects.equals(upperBound, that.upperBound)
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
      this.lowerBound = newLowerBound;
      return this;
    }

    public Builder<T> upperBound(T newUpperBound) {
      this.upperBound = newUpperBound;
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
            type.typeId().javaClass().isInstance(lowerBound),
            "Invalid lower bound type, expected a subtype of %s: %s",
            type.typeId().javaClass().getName(),
            lowerBound.getClass().getName());
      }

      if (null != upperBound) {
        Preconditions.checkArgument(
            null != type, "Invalid type (required when lower bound is set): null");
        Preconditions.checkArgument(
            type.typeId().javaClass().isInstance(upperBound),
            "Invalid upper bound type, expected a subtype of %s: %s",
            type.typeId().javaClass().getName(),
            upperBound.getClass().getName());
      }

      return new BaseFieldStats<>(
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
  }
}
