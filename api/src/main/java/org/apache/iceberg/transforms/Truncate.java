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

package org.apache.iceberg.transforms;

import com.google.common.base.Objects;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import org.apache.iceberg.expressions.BoundPredicate;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.types.Type;

import static org.apache.iceberg.expressions.Expression.Operation.IS_NULL;
import static org.apache.iceberg.expressions.Expression.Operation.LT;
import static org.apache.iceberg.expressions.Expression.Operation.LT_EQ;
import static org.apache.iceberg.expressions.Expression.Operation.NOT_NULL;

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

  public abstract Integer width();

  @Override
  public abstract T apply(T value);

  @Override
  public Type getResultType(Type sourceType) {
    return sourceType;
  }

  private static class TruncateInteger extends Truncate<Integer> {
    private final int width;

    private TruncateInteger(int width) {
      this.width = width;
    }

    @Override
    public Integer width() {
      return width;
    }

    @Override
    public Integer apply(Integer value) {
      return value - (((value % width) + width) % width);
    }

    @Override
    public boolean canTransform(Type type) {
      return type.typeId() == Type.TypeID.INTEGER;
    }

    @Override
    public UnboundPredicate<Integer> project(String name, BoundPredicate<Integer> pred) {
      if (pred.op() == NOT_NULL || pred.op() == IS_NULL) {
        return Expressions.predicate(pred.op(), name);
      }
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
      return width == that.width;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(width);
    }

    @Override
    public String toString() {
      return "truncate[" + width + "]";
    }
  }

  private static class TruncateLong extends Truncate<Long> {
    private final int width;

    private TruncateLong(int width) {
      this.width = width;
    }

    @Override
    public Integer width() {
      return width;
    }

    @Override
    public Long apply(Long value) {
      return value - (((value % width) + width) % width);
    }

    @Override
    public boolean canTransform(Type type) {
      return type.typeId() == Type.TypeID.LONG;
    }

    @Override
    public UnboundPredicate<Long> project(String name, BoundPredicate<Long> pred) {
      if (pred.op() == NOT_NULL || pred.op() == IS_NULL) {
        return Expressions.predicate(pred.op(), name);
      }
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
      return width == that.width;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(width);
    }

    @Override
    public String toString() {
      return "truncate[" + width + "]";
    }
  }

  private static class TruncateString extends Truncate<CharSequence> {
    private final int length;

    private TruncateString(int length) {
      this.length = length;
    }

    @Override
    public Integer width() {
      return length;
    }

    @Override
    public CharSequence apply(CharSequence value) {
      return value.subSequence(0, Math.min(value.length(), length));
    }

    @Override
    public boolean canTransform(Type type) {
      return type.typeId() == Type.TypeID.STRING;
    }

    @Override
    public UnboundPredicate<CharSequence> project(String name,
                                                  BoundPredicate<CharSequence> pred) {
      if (pred.op() == NOT_NULL || pred.op() == IS_NULL) {
        return Expressions.predicate(pred.op(), name);
      }
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
      return length == that.length;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(length);
    }

    @Override
    public String toString() {
      return "truncate[" + length + "]";
    }
  }

  private static class TruncateByteBuffer extends Truncate<ByteBuffer> {
    private final int length;

    private TruncateByteBuffer(int length) {
      this.length = length;
    }

    @Override
    public Integer width() {
      return length;
    }

    @Override
    public ByteBuffer apply(ByteBuffer value) {
      ByteBuffer ret = value.duplicate();
      ret.limit(Math.min(value.limit(), value.position() + length));
      return ret;
    }

    @Override
    public boolean canTransform(Type type) {
      return type.typeId() == Type.TypeID.BINARY;
    }

    @Override
    public UnboundPredicate<ByteBuffer> project(String name,
                                                BoundPredicate<ByteBuffer> pred) {
      if (pred.op() == NOT_NULL || pred.op() == IS_NULL) {
        return Expressions.predicate(pred.op(), name);
      }
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
      return length == that.length;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(length);
    }

    @Override
    public String toHumanString(ByteBuffer value) {
      return value == null ? "null" : TransformUtil.base64encode(value);
    }

    @Override
    public String toString() {
      return "truncate[" + length + "]";
    }
  }

  private static class TruncateDecimal extends Truncate<BigDecimal> {
    private final BigInteger unscaledWidth;

    private TruncateDecimal(int unscaledWidth) {
      this.unscaledWidth = BigInteger.valueOf(unscaledWidth);
    }

    @Override
    public Integer width() {
      return unscaledWidth.intValue();
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
      if (pred.op() == NOT_NULL || pred.op() == IS_NULL) {
        return Expressions.predicate(pred.op(), name);
      }
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
