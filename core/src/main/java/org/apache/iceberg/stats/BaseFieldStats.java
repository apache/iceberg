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
import org.apache.iceberg.StructLike;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Type;

public class BaseFieldStats<T> implements FieldStats<T>, StructLike, Serializable {
  private final transient int fieldId;
  private final transient Type type;
  private final Long columnSize;
  private final Long valueCount;
  private final Long nullValueCount;
  private final Long nanValueCount;
  private final T lowerBound;
  private final T upperBound;

  BaseFieldStats(
      int fieldId,
      Type type,
      Long columnSize,
      Long valueCount,
      Long nullValueCount,
      Long nanValueCount,
      T lowerBound,
      T upperBound) {
    this.fieldId = fieldId;
    this.type = type;
    this.columnSize = columnSize;
    this.valueCount = valueCount;
    this.nullValueCount = nullValueCount;
    this.nanValueCount = nanValueCount;
    this.lowerBound = lowerBound;
    this.upperBound = upperBound;
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
  public Long columnSize() {
    return columnSize;
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
  public T lowerBound() {
    return lowerBound;
  }

  @Override
  public T upperBound() {
    return upperBound;
  }

  @Override
  public int size() {
    return 6;
  }

  @Override
  public <X> X get(int pos, Class<X> javaClass) {
    switch (pos) {
      case StatsUtil.COLUMN_SIZE_OFFSET:
        return javaClass.cast(columnSize);
      case StatsUtil.VALUE_COUNT_OFFSET:
        return javaClass.cast(valueCount);
      case StatsUtil.NULL_VALUE_COUNT_OFFSET:
        return javaClass.cast(nullValueCount);
      case StatsUtil.NAN_VALUE_COUNT_OFFSET:
        return javaClass.cast(nanValueCount);
      case StatsUtil.LOWER_BOUND_OFFSET:
        return javaClass.cast(lowerBound);
      case StatsUtil.UPPER_BOUND_OFFSET:
        return javaClass.cast(upperBound);
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
        .add("columnSize", columnSize)
        .add("valueCount", valueCount)
        .add("nullValueCount", nullValueCount)
        .add("nanValueCount", nanValueCount)
        .add("lowerBound", lowerBound)
        .add("upperBound", upperBound)
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
        && Objects.equals(columnSize, that.columnSize)
        && Objects.equals(valueCount, that.valueCount)
        && Objects.equals(nullValueCount, that.nullValueCount)
        && Objects.equals(nanValueCount, that.nanValueCount)
        && Objects.equals(lowerBound, that.lowerBound)
        && Objects.equals(upperBound, that.upperBound);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        fieldId,
        type,
        columnSize,
        valueCount,
        nullValueCount,
        nanValueCount,
        lowerBound,
        upperBound);
  }

  public static <X> Builder<X> builder() {
    return new Builder<>();
  }

  public static <X> Builder<X> buildFrom(FieldStats<X> value) {
    Preconditions.checkArgument(null != value, "Invalid column stats: null");
    return BaseFieldStats.<X>builder()
        .columnSize(value.columnSize())
        .valueCount(value.valueCount())
        .nanValueCount(value.nanValueCount())
        .nullValueCount(value.nullValueCount())
        .type(value.type())
        .fieldId(value.fieldId())
        .lowerBound(value.lowerBound())
        .upperBound(value.upperBound());
  }

  public static class Builder<T> {
    private int fieldId;
    private Type type;
    private Long columnSize;
    private Long valueCount;
    private Long nullValueCount;
    private Long nanValueCount;
    private T lowerBound;
    private T upperBound;

    private Builder() {}

    public Builder<T> type(Type newType) {
      this.type = newType;
      return this;
    }

    public Builder<T> columnSize(Long newColumnSize) {
      this.columnSize = newColumnSize;
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

    public BaseFieldStats<T> build() {
      return new BaseFieldStats<>(
          fieldId,
          type,
          columnSize,
          valueCount,
          nullValueCount,
          nanValueCount,
          lowerBound,
          upperBound);
    }
  }
}
