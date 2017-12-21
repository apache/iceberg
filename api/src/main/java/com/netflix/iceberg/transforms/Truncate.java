/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.iceberg.transforms;

import com.google.common.base.Objects;
import com.netflix.iceberg.expressions.BoundPredicate;
import com.netflix.iceberg.expressions.Expressions;
import com.netflix.iceberg.expressions.UnboundPredicate;
import com.netflix.iceberg.types.Type;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

import static com.netflix.iceberg.expressions.Expression.Operation.LT;
import static com.netflix.iceberg.expressions.Expression.Operation.LT_EQ;

abstract class Truncate<T> implements Transform<T, T> {
  @SuppressWarnings("unchecked")
  static <T> Truncate<T> get(Type type, int width) {
    switch (type.typeId()) {
      case INTEGER:
        return (Truncate<T>) new TruncateInteger(width);
      case LONG:
        return (Truncate<T>) new TruncateLong(width);
      case DECIMAL:
        return (Truncate<T>) new TruncateDecimal(width);
      case STRING:
        return (Truncate<T>) new TruncateString(width);
      case BINARY:
        return (Truncate<T>) new TruncateByteBuffer(width);
      default:
        throw new UnsupportedOperationException(
            "Cannot truncate type: " + type);
    }
  }

  @Override
  abstract public T apply(T value);

  @Override
  public Type getResultType(Type sourceType) {
    return sourceType;
  }

  private static class TruncateInteger extends Truncate<Integer> {
    private final int W;

    private TruncateInteger(int width) {
      this.W = width;
    }

    @Override
    public Integer apply(Integer value) {
      return value - (((value % W) + W) % W);
    }

    @Override
    public boolean canTransform(Type type) {
      return type.typeId() == Type.TypeID.INTEGER;
    }

    @Override
    public UnboundPredicate<Integer> project(String name, BoundPredicate<Integer> pred) {
      return ProjectionUtil.truncateInteger(name, pred, this);
    }

    @Override
    public UnboundPredicate<Integer> projectStrict(String name, BoundPredicate<Integer> predicate) {
      // TODO: for integers, can this return the original predicate?
      // No. the predicate needs to be in terms of the applied value. For all x, apply(x) <= x.
      // Therefore, the lower bound can be transformed outside of a greater-than bound.
      int in;
      int out;
      int inImage;
      int outImage;
      switch (predicate.op()) {
        case LT:
          in = predicate.literal().value() - 1;
          out = predicate.literal().value();
          inImage = apply(in);
          outImage = apply(out);
          if (inImage != outImage) {
            return Expressions.predicate(LT_EQ, name, inImage);
          } else {
            return Expressions.predicate(LT, name, inImage);
          }
        case LT_EQ:
          in = predicate.literal().value();
          out = predicate.literal().value() + 1;
          inImage = apply(in);
          outImage = apply(out);
          if (inImage != outImage) {
            return Expressions.predicate(LT_EQ, name, inImage);
          } else {
            return Expressions.predicate(LT, name, inImage);
          }
        case GT:
        case GT_EQ:
        case EQ:
        case NOT_EQ:
//        case IN:
//          break;
//        case NOT_IN:
//          break;
        default:
          return null;
      }
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      TruncateInteger that = (TruncateInteger) o;
      return W == that.W;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(W);
    }

    @Override
    public String toString() {
      return "truncate[" + W + "]";
    }
  }

  private static class TruncateLong extends Truncate<Long> {
    private final int W;

    private TruncateLong(int width) {
      this.W = width;
    }

    @Override
    public Long apply(Long value) {
      return value - (((value % W) + W) % W);
    }

    @Override
    public boolean canTransform(Type type) {
      return type.typeId() == Type.TypeID.LONG;
    }

    @Override
    public UnboundPredicate<Long> project(String name, BoundPredicate<Long> pred) {
      return ProjectionUtil.truncateLong(name, pred, this);
    }

    @Override
    public UnboundPredicate<Long> projectStrict(String name, BoundPredicate<Long> predicate) {
      return null;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      TruncateLong that = (TruncateLong) o;
      return W == that.W;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(W);
    }

    @Override
    public String toString() {
      return "truncate[" + W + "]";
    }
  }

  private static class TruncateString extends Truncate<CharSequence> {
    private final int L;

    private TruncateString(int length) {
      this.L = length;
    }

    @Override
    public CharSequence apply(CharSequence value) {
      return value.subSequence(0, Math.min(value.length(), L));
    }

    @Override
    public boolean canTransform(Type type) {
      return type.typeId() == Type.TypeID.STRING;
    }

    @Override
    public UnboundPredicate<CharSequence> project(String name,
                                                  BoundPredicate<CharSequence> pred) {
      return ProjectionUtil.truncateArray(name, pred, this);
    }

    @Override
    public UnboundPredicate<CharSequence> projectStrict(String name,
                                                        BoundPredicate<CharSequence> predicate) {
      return null;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      TruncateString that = (TruncateString) o;
      return L == that.L;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(L);
    }

    @Override
    public String toString() {
      return "truncate[" + L + "]";
    }
  }

  private static class TruncateByteBuffer extends Truncate<ByteBuffer> {
    private final int L;

    private TruncateByteBuffer(int length) {
      this.L = length;
    }

    @Override
    public ByteBuffer apply(ByteBuffer value) {
      ByteBuffer ret = value.duplicate();
      ret.limit(Math.min(value.limit(), value.position() + L));
      return ret;
    }

    @Override
    public boolean canTransform(Type type) {
      return type.typeId() == Type.TypeID.BINARY;
    }

    @Override
    public UnboundPredicate<ByteBuffer> project(String name,
                                                BoundPredicate<ByteBuffer> pred) {
      return ProjectionUtil.truncateArray(name, pred, this);
    }

    @Override
    public UnboundPredicate<ByteBuffer> projectStrict(String name,
                                                      BoundPredicate<ByteBuffer> predicate) {
      return null;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      TruncateByteBuffer that = (TruncateByteBuffer) o;
      return L == that.L;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(L);
    }

    @Override
    public String toHumanString(ByteBuffer value) {
      return value == null ? "null" : TransformUtil.base64encode(value);
    }

    @Override
    public String toString() {
      return "truncate[" + L + "]";
    }
  }

  private static class TruncateDecimal extends Truncate<BigDecimal> {
    private final BigInteger unscaledWidth;

    private TruncateDecimal(int unscaledWidth) {
      this.unscaledWidth = BigInteger.valueOf(unscaledWidth);
    }

    @Override
    public BigDecimal apply(BigDecimal value) {
      BigDecimal remainder = new BigDecimal(
          value.unscaledValue()
              .remainder(unscaledWidth)
              .add(unscaledWidth)
              .remainder(unscaledWidth),
          value.scale());
      return value.subtract(remainder);
    }

    @Override
    public boolean canTransform(Type type) {
      return type.typeId() == Type.TypeID.DECIMAL;
    }

    @Override
    public UnboundPredicate<BigDecimal> project(String name,
                                                BoundPredicate<BigDecimal> pred) {
      return ProjectionUtil.truncateDecimal(name, pred, this);
    }

    @Override
    public UnboundPredicate<BigDecimal> projectStrict(String name,
                                                      BoundPredicate<BigDecimal> predicate) {
      return null;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      TruncateDecimal that = (TruncateDecimal) o;
      return unscaledWidth.equals(that.unscaledWidth);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(unscaledWidth);
    }

    @Override
    public String toString() {
      return "truncate[" + unscaledWidth + "]";
    }
  }
}
