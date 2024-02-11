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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.function.Function;
import org.apache.iceberg.expressions.BoundLiteralPredicate;
import org.apache.iceberg.expressions.BoundPredicate;
import org.apache.iceberg.expressions.BoundTransform;
import org.apache.iceberg.expressions.BoundUnaryPredicate;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.relocated.com.google.common.base.Objects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.util.BinaryUtil;
import org.apache.iceberg.util.SerializableFunction;
import org.apache.iceberg.util.TruncateUtil;
import org.apache.iceberg.util.UnicodeUtil;

class Truncate<T> implements Transform<T, T>, Function<T, T> {
  static <T> Truncate<T> get(int width) {
    Preconditions.checkArgument(width > 0, "Invalid truncate width: %s (must be > 0)", width);
    return new Truncate<>(width);
  }

  /** @deprecated will be removed in 2.0.0 */
  @Deprecated
  @SuppressWarnings("unchecked")
  static <T, R extends Truncate<T> & SerializableFunction<T, T>> R get(Type type, int width) {
    Preconditions.checkArgument(width > 0, "Invalid truncate width: %s (must be > 0)", width);

    switch (type.typeId()) {
      case INTEGER:
        return (R) new TruncateInteger(width);
      case LONG:
        return (R) new TruncateLong(width);
      case DECIMAL:
        return (R) new TruncateDecimal(width);
      case STRING:
        return (R) new TruncateString(width);
      case BINARY:
        return (R) new TruncateByteBuffer(width);
      default:
        throw new UnsupportedOperationException("Cannot truncate type: " + type);
    }
  }

  @SuppressWarnings("checkstyle:VisibilityModifier")
  protected final int width;

  Truncate(int width) {
    this.width = width;
  }

  public Integer width() {
    return width;
  }

  @Override
  public T apply(T value) {
    throw new UnsupportedOperationException(
        "apply(value) is deprecated, use bind(Type).apply(value)");
  }

  @Override
  public SerializableFunction<T, T> bind(Type type) {
    Preconditions.checkArgument(canTransform(type), "Cannot bind to unsupported type: %s", type);
    return (SerializableFunction<T, T>) get(type, width);
  }

  @Override
  public boolean canTransform(Type type) {
    switch (type.typeId()) {
      case INTEGER:
      case LONG:
      case STRING:
      case BINARY:
      case DECIMAL:
        return true;
    }
    return false;
  }

  @Override
  public UnboundPredicate<T> project(String name, BoundPredicate<T> predicate) {
    Truncate<T> bound = (Truncate<T>) get(predicate.term().type(), width);
    return bound.project(name, predicate);
  }

  @Override
  public UnboundPredicate<T> projectStrict(String name, BoundPredicate<T> predicate) {
    Truncate<T> bound = (Truncate<T>) get(predicate.term().type(), width);
    return bound.projectStrict(name, predicate);
  }

  @Override
  public Type getResultType(Type sourceType) {
    return sourceType;
  }

  @Override
  public boolean preservesOrder() {
    return true;
  }

  @Override
  public boolean satisfiesOrderOf(Transform<?, ?> other) {
    if (this == other) {
      return true;
    }

    if (!(other instanceof Truncate)) {
      return false;
    }

    Truncate<?> otherTrunc = (Truncate<?>) other;
    return otherTrunc.width <= width;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (!(o instanceof Truncate)) {
      return false;
    }

    Truncate<?> that = (Truncate<?>) o;
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

  private static class TruncateInteger extends Truncate<Integer>
      implements SerializableFunction<Integer, Integer> {

    private TruncateInteger(int width) {
      super(width);
    }

    @Override
    public SerializableFunction<Integer, Integer> bind(Type type) {
      Preconditions.checkArgument(
          type.typeId() == Type.TypeID.INTEGER,
          "Cannot bind truncate to a different type: %s",
          type);
      return this;
    }

    @Override
    public Integer apply(Integer value) {
      if (value == null) {
        return null;
      }

      return TruncateUtil.truncateInt(width, value);
    }

    @Override
    public UnboundPredicate<Integer> project(String name, BoundPredicate<Integer> pred) {
      if (pred.term() instanceof BoundTransform) {
        return ProjectionUtil.projectTransformPredicate(this, name, pred);
      }

      if (pred.isUnaryPredicate()) {
        return Expressions.predicate(pred.op(), name);
      } else if (pred.isLiteralPredicate()) {
        return ProjectionUtil.truncateInteger(name, pred.asLiteralPredicate(), this);
      } else if (pred.isSetPredicate() && pred.op() == Expression.Operation.IN) {
        return ProjectionUtil.transformSet(name, pred.asSetPredicate(), this);
      }
      return null;
    }

    @Override
    public UnboundPredicate<Integer> projectStrict(String name, BoundPredicate<Integer> pred) {
      if (pred.term() instanceof BoundTransform) {
        return ProjectionUtil.projectTransformPredicate(this, name, pred);
      }

      // TODO: for integers, can this return the original predicate?
      // No. the predicate needs to be in terms of the applied value. For all x, apply(x) <= x.
      // Therefore, the lower bound can be transformed outside of a greater-than bound.
      if (pred instanceof BoundUnaryPredicate) {
        return Expressions.predicate(pred.op(), name);
      } else if (pred instanceof BoundLiteralPredicate) {
        return ProjectionUtil.truncateIntegerStrict(name, pred.asLiteralPredicate(), this);
      } else if (pred.isSetPredicate() && pred.op() == Expression.Operation.NOT_IN) {
        return ProjectionUtil.transformSet(name, pred.asSetPredicate(), this);
      }
      return null;
    }
  }

  private static class TruncateLong extends Truncate<Long>
      implements SerializableFunction<Long, Long> {

    private TruncateLong(int width) {
      super(width);
    }

    @Override
    public SerializableFunction<Long, Long> bind(Type type) {
      Preconditions.checkArgument(
          type.typeId() == Type.TypeID.LONG, "Cannot bind truncate to a different type: %s", type);
      return this;
    }

    @Override
    public Long apply(Long value) {
      if (value == null) {
        return null;
      }

      return TruncateUtil.truncateLong(width, value);
    }

    @Override
    public UnboundPredicate<Long> project(String name, BoundPredicate<Long> pred) {
      if (pred.term() instanceof BoundTransform) {
        return ProjectionUtil.projectTransformPredicate(this, name, pred);
      }

      if (pred.isUnaryPredicate()) {
        return Expressions.predicate(pred.op(), name);
      } else if (pred.isLiteralPredicate()) {
        return ProjectionUtil.truncateLong(name, pred.asLiteralPredicate(), this);
      } else if (pred.isSetPredicate() && pred.op() == Expression.Operation.IN) {
        return ProjectionUtil.transformSet(name, pred.asSetPredicate(), this);
      }
      return null;
    }

    @Override
    public UnboundPredicate<Long> projectStrict(String name, BoundPredicate<Long> pred) {
      if (pred.term() instanceof BoundTransform) {
        return ProjectionUtil.projectTransformPredicate(this, name, pred);
      }

      if (pred.isUnaryPredicate()) {
        return Expressions.predicate(pred.op(), name);
      } else if (pred.isLiteralPredicate()) {
        return ProjectionUtil.truncateLongStrict(name, pred.asLiteralPredicate(), this);
      } else if (pred.isSetPredicate() && pred.op() == Expression.Operation.NOT_IN) {
        return ProjectionUtil.transformSet(name, pred.asSetPredicate(), this);
      }
      return null;
    }
  }

  private static class TruncateString extends Truncate<CharSequence>
      implements SerializableFunction<CharSequence, CharSequence> {

    private TruncateString(int length) {
      super(length);
    }

    @Override
    public SerializableFunction<CharSequence, CharSequence> bind(Type type) {
      Preconditions.checkArgument(
          type.typeId() == Type.TypeID.STRING,
          "Cannot bind truncate to a different type: %s",
          type);
      return this;
    }

    @Override
    public CharSequence apply(CharSequence value) {
      if (value == null) {
        return null;
      }

      return UnicodeUtil.truncateString(value, width);
    }

    @Override
    public boolean satisfiesOrderOf(Transform<?, ?> other) {
      if (this == other) {
        return true;
      } else if (other instanceof TruncateString) {
        TruncateString otherTransform = (TruncateString) other;
        return width() >= otherTransform.width();
      }

      return false;
    }

    @Override
    public UnboundPredicate<CharSequence> project(
        String name, BoundPredicate<CharSequence> predicate) {
      if (predicate.term() instanceof BoundTransform) {
        return ProjectionUtil.projectTransformPredicate(this, name, predicate);
      }

      if (predicate.isUnaryPredicate()) {
        return Expressions.predicate(predicate.op(), name);
      } else if (predicate.isLiteralPredicate()) {
        BoundLiteralPredicate<CharSequence> pred = predicate.asLiteralPredicate();
        switch (pred.op()) {
          case STARTS_WITH:
          case ENDS_WITH:
          case CONTAINS:
            if (pred.literal().value().length() < width()) {
              return Expressions.predicate(pred.op(), name, pred.literal().value());
            } else if (pred.literal().value().length() == width()) {
              return Expressions.equal(name, pred.literal().value());
            }

            return ProjectionUtil.truncateArray(name, pred, this);

          case NOT_STARTS_WITH:
          case NOT_ENDS_WITH:
          case NOT_CONTAINS:
            if (pred.literal().value().length() < width()) {
              return Expressions.predicate(pred.op(), name, pred.literal().value());
            } else if (pred.literal().value().length() == width()) {
              return Expressions.notEqual(name, pred.literal().value());
            }

            return null;

          default:
            return ProjectionUtil.truncateArray(name, pred, this);
        }
      } else if (predicate.isSetPredicate() && predicate.op() == Expression.Operation.IN) {
        return ProjectionUtil.transformSet(name, predicate.asSetPredicate(), this);
      }
      return null;
    }

    @Override
    public UnboundPredicate<CharSequence> projectStrict(
        String name, BoundPredicate<CharSequence> predicate) {
      if (predicate.term() instanceof BoundTransform) {
        return ProjectionUtil.projectTransformPredicate(this, name, predicate);
      }

      if (predicate instanceof BoundUnaryPredicate) {
        return Expressions.predicate(predicate.op(), name);
      } else if (predicate instanceof BoundLiteralPredicate) {
        BoundLiteralPredicate<CharSequence> pred = predicate.asLiteralPredicate();
        switch (pred.op()) {
          case STARTS_WITH:
          case ENDS_WITH:
          case CONTAINS:
            if (pred.literal().value().length() < width()) {
              return Expressions.predicate(pred.op(), name, pred.literal().value());
            } else if (pred.literal().value().length() == width()) {
              return Expressions.equal(name, pred.literal().value());
            }

            return null;

          case NOT_STARTS_WITH:
          case NOT_ENDS_WITH:
          case NOT_CONTAINS:
            if (pred.literal().value().length() < width()) {
              return Expressions.predicate(pred.op(), name, pred.literal().value());
            } else if (pred.literal().value().length() == width()) {
              return Expressions.notEqual(name, pred.literal().value());
            }

            return Expressions.predicate(pred.op(), name, apply(pred.literal().value()).toString());

          default:
            return ProjectionUtil.truncateArrayStrict(name, pred, this);
        }
      } else if (predicate.isSetPredicate() && predicate.op() == Expression.Operation.NOT_IN) {
        return ProjectionUtil.transformSet(name, predicate.asSetPredicate(), this);
      }
      return null;
    }
  }

  private static class TruncateByteBuffer extends Truncate<ByteBuffer>
      implements SerializableFunction<ByteBuffer, ByteBuffer> {

    private TruncateByteBuffer(int length) {
      super(length);
    }

    @Override
    public SerializableFunction<ByteBuffer, ByteBuffer> bind(Type type) {
      Preconditions.checkArgument(
          type.typeId() == Type.TypeID.BINARY || type.typeId() == Type.TypeID.FIXED,
          "Cannot bind truncate to a different type: %s",
          type);
      return this;
    }

    @Override
    public ByteBuffer apply(ByteBuffer value) {
      if (value == null) {
        return null;
      }

      return BinaryUtil.truncateBinaryUnsafe(value, width);
    }

    @Override
    public UnboundPredicate<ByteBuffer> project(String name, BoundPredicate<ByteBuffer> pred) {
      if (pred.term() instanceof BoundTransform) {
        return ProjectionUtil.projectTransformPredicate(this, name, pred);
      }

      if (pred.isUnaryPredicate()) {
        return Expressions.predicate(pred.op(), name);
      } else if (pred.isLiteralPredicate()) {
        return ProjectionUtil.truncateArray(name, pred.asLiteralPredicate(), this);
      } else if (pred.isSetPredicate() && pred.op() == Expression.Operation.IN) {
        return ProjectionUtil.transformSet(name, pred.asSetPredicate(), this);
      }
      return null;
    }

    @Override
    public UnboundPredicate<ByteBuffer> projectStrict(
        String name, BoundPredicate<ByteBuffer> pred) {
      if (pred.term() instanceof BoundTransform) {
        return ProjectionUtil.projectTransformPredicate(this, name, pred);
      }

      if (pred.isUnaryPredicate()) {
        return Expressions.predicate(pred.op(), name);
      } else if (pred.isLiteralPredicate()) {
        return ProjectionUtil.truncateArrayStrict(name, pred.asLiteralPredicate(), this);
      } else if (pred.isSetPredicate() && pred.op() == Expression.Operation.NOT_IN) {
        return ProjectionUtil.transformSet(name, pred.asSetPredicate(), this);
      }
      return null;
    }
  }

  private static class TruncateDecimal extends Truncate<BigDecimal>
      implements SerializableFunction<BigDecimal, BigDecimal> {

    private final BigInteger unscaledWidth;

    private TruncateDecimal(int unscaledWidth) {
      super(unscaledWidth);
      this.unscaledWidth = BigInteger.valueOf(unscaledWidth);
    }

    @Override
    public SerializableFunction<BigDecimal, BigDecimal> bind(Type type) {
      Preconditions.checkArgument(
          type.typeId() == Type.TypeID.DECIMAL,
          "Cannot bind truncate to a different type: %s",
          type);
      return this;
    }

    @Override
    public BigDecimal apply(BigDecimal value) {
      if (value == null) {
        return null;
      }

      return TruncateUtil.truncateDecimal(unscaledWidth, value);
    }

    @Override
    public UnboundPredicate<BigDecimal> project(String name, BoundPredicate<BigDecimal> pred) {
      if (pred.term() instanceof BoundTransform) {
        return ProjectionUtil.projectTransformPredicate(this, name, pred);
      }

      if (pred.isUnaryPredicate()) {
        return Expressions.predicate(pred.op(), name);
      } else if (pred.isLiteralPredicate()) {
        return ProjectionUtil.truncateDecimal(name, pred.asLiteralPredicate(), this);
      } else if (pred.isSetPredicate() && pred.op() == Expression.Operation.IN) {
        return ProjectionUtil.transformSet(name, pred.asSetPredicate(), this);
      }
      return null;
    }

    @Override
    public UnboundPredicate<BigDecimal> projectStrict(
        String name, BoundPredicate<BigDecimal> pred) {
      if (pred.term() instanceof BoundTransform) {
        return ProjectionUtil.projectTransformPredicate(this, name, pred);
      }

      if (pred.isUnaryPredicate()) {
        return Expressions.predicate(pred.op(), name);
      } else if (pred.isLiteralPredicate()) {
        return ProjectionUtil.truncateDecimalStrict(name, pred.asLiteralPredicate(), this);
      } else if (pred.isSetPredicate() && pred.op() == Expression.Operation.NOT_IN) {
        return ProjectionUtil.transformSet(name, pred.asSetPredicate(), this);
      }
      return null;
    }
  }
}
