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
package org.apache.iceberg.rest.restrictions;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.UUID;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.io.BaseEncoding;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.iceberg.util.SerializableFunction;

/**
 * Factory for binding {@link Action}s to Iceberg {@link Type}s, producing serializable functions
 * that implement the masking/transformation semantics defined by the ReadRestrictions spec.
 *
 * <p>Follows the {@code Transforms}/{@code Expressions} naming convention. The returned functions
 * operate on Iceberg's Java type representations (String, Integer, Long, ByteBuffer, BigDecimal)
 * and are engine-agnostic. Engine integrations (Spark, Flink, etc.) wrap these in thin adapter
 * expressions that handle internal type conversion.
 */
public class Actions {

  private Actions() {}

  public static SerializableFunction<Object, Object> bind(Action action, Type type) {
    return bind(action, type, null);
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  public static SerializableFunction<Object, Object> bind(Action action, Type type, byte[] salt) {
    if (action instanceof Action.ReplaceWithNull) {
      return ReplaceWithNullFunction.INSTANCE;
    } else if (action instanceof Action.ApplyExpression) {
      return ApplyExpressionFunction.INSTANCE;
    } else if (action instanceof Action.MaskAlphanum) {
      Preconditions.checkArgument(
          type.typeId() == Type.TypeID.STRING, "mask-alphanum requires STRING type, got %s", type);
      return MaskAlphanumFunction.INSTANCE;
    } else if (action instanceof Action.ShowFirst4) {
      Preconditions.checkArgument(
          type.typeId() == Type.TypeID.STRING, "show-first-4 requires STRING type, got %s", type);
      return ShowFirst4Function.INSTANCE;
    } else if (action instanceof Action.ShowLast4) {
      Preconditions.checkArgument(
          type.typeId() == Type.TypeID.STRING, "show-last-4 requires STRING type, got %s", type);
      return ShowLast4Function.INSTANCE;
    } else if (action instanceof Action.MaskToDefault) {
      return new MaskToDefaultFunction(defaultValueFor(type));
    } else if (action instanceof Action.TruncateToYear) {
      return bindTruncateToYear(type);
    } else if (action instanceof Action.TruncateToMonth) {
      return bindTruncateToMonth(type);
    } else if (action instanceof Action.Sha256Global) {
      return new Sha256Function(type.typeId(), null);
    } else if (action instanceof Action.Sha256QueryLocal) {
      Preconditions.checkArgument(
          salt != null && salt.length >= 16, "sha-256-query-local salt must be >= 16 bytes");
      return new Sha256Function(type.typeId(), salt);
    } else if (action instanceof Action.Unknown) {
      throw new IllegalStateException(
          "Cannot bind unknown action type '"
              + action.actionType()
              + "': this client does not recognize the action. Upgrade the client or remove the "
              + "action from the server-side policy.");
    }

    throw new IllegalArgumentException("Unrecognized action: " + action.actionType());
  }

  private static SerializableFunction<Object, Object> bindTruncateToYear(Type type) {
    switch (type.typeId()) {
      case DATE:
        return TruncateToYearDateFunction.INSTANCE;
      case TIMESTAMP:
        return TruncateToYearTimestampFunction.INSTANCE;
      case TIMESTAMP_NANO:
        return TruncateToYearTimestampNanoFunction.INSTANCE;
      default:
        throw new UnsupportedOperationException(
            "truncate-to-year is not supported for type: " + type);
    }
  }

  private static SerializableFunction<Object, Object> bindTruncateToMonth(Type type) {
    switch (type.typeId()) {
      case DATE:
        return TruncateToMonthDateFunction.INSTANCE;
      case TIMESTAMP:
        return TruncateToMonthTimestampFunction.INSTANCE;
      case TIMESTAMP_NANO:
        return TruncateToMonthTimestampNanoFunction.INSTANCE;
      default:
        throw new UnsupportedOperationException(
            "truncate-to-month is not supported for type: " + type);
    }
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  private static Object defaultValueFor(Type type) {
    switch (type.typeId()) {
      case BOOLEAN:
        return Boolean.FALSE;
      case INTEGER:
        return INT_DEFAULT;
      case LONG:
        return LONG_DEFAULT;
      case FLOAT:
        return FLOAT_DEFAULT;
      case DOUBLE:
        return DOUBLE_DEFAULT;
      case STRING:
        return STRING_DEFAULT;
      case DATE:
        return DATE_DEFAULT;
      case TIME:
        return TIME_DEFAULT_MICROS;
      case TIMESTAMP:
        return TIMESTAMP_DEFAULT_MICROS;
      case TIMESTAMP_NANO:
        return TIMESTAMP_DEFAULT_NANOS;
      case UUID:
        return UUID_DEFAULT;
      case FIXED:
        int fixedLen = ((Types.FixedType) type).length();
        return ByteBuffer.allocate(fixedLen).asReadOnlyBuffer();
      case BINARY:
        return EMPTY_BUFFER;
      case DECIMAL:
        int scale = ((Types.DecimalType) type).scale();
        return new BigDecimal(BigInteger.ZERO, scale);
      default:
        throw new UnsupportedOperationException(
            "mask-to-default is not implemented for type: " + type);
    }
  }

  private static final Integer INT_DEFAULT = 999999999;
  private static final Long LONG_DEFAULT = 999999999L;
  private static final Float FLOAT_DEFAULT = 0.0f;
  private static final Double DOUBLE_DEFAULT = 0.0d;
  private static final String STRING_DEFAULT = "XXXXXXXX";
  private static final Integer DATE_DEFAULT = (int) LocalDate.of(9999, 12, 31).toEpochDay();
  private static final Long TIME_DEFAULT_MICROS = 0L;
  private static final Long TIMESTAMP_DEFAULT_MICROS =
      DateTimeUtil.microsFromTimestamp(LocalDateTime.of(9999, 12, 31, 0, 0));
  // Per spec: nanos types use 2261-12-31 because 9999 overflows Long nanoseconds from epoch.
  private static final Long TIMESTAMP_DEFAULT_NANOS =
      DateTimeUtil.nanosFromTimestamp(LocalDateTime.of(2261, 12, 31, 0, 0));
  private static final UUID UUID_DEFAULT = UUID.fromString("00000000-0000-0000-0000-000000000000");
  private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0).asReadOnlyBuffer();

  // ---- code-point masking helpers ----

  static int mapCodePoint(int cp) {
    if (cp >= 0x30 && cp <= 0x39) {
      return 'n';
    } else if (cp == '(' || cp == ')' || cp == ',' || cp == '.' || cp == '-' || cp == '@') {
      return cp;
    } else {
      return 'x';
    }
  }

  // ==== Inner function classes ====

  /**
   * Base for masking functions where null input must pass through as null unchanged (spec: "For all
   * actions, if the input column value is NULL, the output MUST be NULL."). Subclasses implement
   * {@link #applyNonNull(Object)} and don't have to repeat the guard.
   */
  private abstract static class NullSafeFunction implements SerializableFunction<Object, Object> {
    @Override
    public final Object apply(Object value) {
      return value == null ? null : applyNonNull(value);
    }

    abstract Object applyNonNull(Object value);
  }

  private static class MaskAlphanumFunction extends NullSafeFunction {
    static final MaskAlphanumFunction INSTANCE = new MaskAlphanumFunction();

    @Override
    Object applyNonNull(Object value) {
      String input = value.toString();
      StringBuilder sb = new StringBuilder(input.length());
      int offset = 0;
      while (offset < input.length()) {
        int cp = input.codePointAt(offset);
        sb.appendCodePoint(mapCodePoint(cp));
        offset += Character.charCount(cp);
      }
      return sb.toString();
    }
  }

  private static class ShowFirst4Function extends NullSafeFunction {
    static final ShowFirst4Function INSTANCE = new ShowFirst4Function();

    @Override
    Object applyNonNull(Object value) {
      String input = value.toString();
      if (input.codePointCount(0, input.length()) <= 4) {
        return input;
      }
      StringBuilder sb = new StringBuilder(input.length());
      int cpIndex = 0;
      int offset = 0;
      while (offset < input.length()) {
        int cp = input.codePointAt(offset);
        sb.appendCodePoint(cpIndex < 4 ? cp : mapCodePoint(cp));
        offset += Character.charCount(cp);
        cpIndex++;
      }
      return sb.toString();
    }
  }

  private static class ShowLast4Function extends NullSafeFunction {
    static final ShowLast4Function INSTANCE = new ShowLast4Function();

    @Override
    Object applyNonNull(Object value) {
      String input = value.toString();
      int totalCps = input.codePointCount(0, input.length());
      if (totalCps <= 4) {
        return input;
      }
      int keepFrom = totalCps - 4;
      StringBuilder sb = new StringBuilder(input.length());
      int cpIndex = 0;
      int offset = 0;
      while (offset < input.length()) {
        int cp = input.codePointAt(offset);
        sb.appendCodePoint(cpIndex >= keepFrom ? cp : mapCodePoint(cp));
        offset += Character.charCount(cp);
        cpIndex++;
      }
      return sb.toString();
    }
  }

  private static class ReplaceWithNullFunction implements SerializableFunction<Object, Object> {
    static final ReplaceWithNullFunction INSTANCE = new ReplaceWithNullFunction();

    @Override
    public Object apply(Object value) {
      return null;
    }
  }

  private static class ApplyExpressionFunction implements SerializableFunction<Object, Object> {
    static final ApplyExpressionFunction INSTANCE = new ApplyExpressionFunction();

    @Override
    public Object apply(Object value) {
      throw new UnsupportedOperationException(
          "apply-expression column projection is not supported by this client "
              + "(Iceberg Expression is currently boolean-only)");
    }
  }

  private static class MaskToDefaultFunction extends NullSafeFunction {
    private final Object defaultValue;

    MaskToDefaultFunction(Object defaultValue) {
      this.defaultValue = defaultValue;
    }

    @Override
    Object applyNonNull(Object value) {
      if (defaultValue instanceof ByteBuffer) {
        return ((ByteBuffer) defaultValue).duplicate();
      }
      return defaultValue;
    }
  }

  private static class TruncateToYearDateFunction extends NullSafeFunction {
    static final TruncateToYearDateFunction INSTANCE = new TruncateToYearDateFunction();

    @Override
    Object applyNonNull(Object value) {
      int days = (Integer) value;
      LocalDate truncated = DateTimeUtil.dateFromDays(days).withMonth(1).withDayOfMonth(1);
      return DateTimeUtil.daysFromDate(truncated);
    }
  }

  private static class TruncateToYearTimestampFunction extends NullSafeFunction {
    static final TruncateToYearTimestampFunction INSTANCE = new TruncateToYearTimestampFunction();

    @Override
    Object applyNonNull(Object value) {
      long micros = (Long) value;
      LocalDateTime ldt = DateTimeUtil.timestampFromMicros(micros);
      LocalDateTime truncated =
          ldt.withMonth(1).withDayOfMonth(1).withHour(0).withMinute(0).withSecond(0).withNano(0);
      return DateTimeUtil.microsFromTimestamp(truncated);
    }
  }

  private static class TruncateToYearTimestampNanoFunction extends NullSafeFunction {
    static final TruncateToYearTimestampNanoFunction INSTANCE =
        new TruncateToYearTimestampNanoFunction();

    @Override
    Object applyNonNull(Object value) {
      long nanos = (Long) value;
      LocalDateTime ldt = DateTimeUtil.timestampFromNanos(nanos);
      LocalDateTime truncated =
          ldt.withMonth(1).withDayOfMonth(1).withHour(0).withMinute(0).withSecond(0).withNano(0);
      return DateTimeUtil.nanosFromTimestamp(truncated);
    }
  }

  private static class TruncateToMonthDateFunction extends NullSafeFunction {
    static final TruncateToMonthDateFunction INSTANCE = new TruncateToMonthDateFunction();

    @Override
    Object applyNonNull(Object value) {
      int days = (Integer) value;
      LocalDate truncated = DateTimeUtil.dateFromDays(days).withDayOfMonth(1);
      return DateTimeUtil.daysFromDate(truncated);
    }
  }

  private static class TruncateToMonthTimestampFunction extends NullSafeFunction {
    static final TruncateToMonthTimestampFunction INSTANCE = new TruncateToMonthTimestampFunction();

    @Override
    Object applyNonNull(Object value) {
      long micros = (Long) value;
      LocalDateTime ldt = DateTimeUtil.timestampFromMicros(micros);
      LocalDateTime truncated =
          ldt.withDayOfMonth(1).withHour(0).withMinute(0).withSecond(0).withNano(0);
      return DateTimeUtil.microsFromTimestamp(truncated);
    }
  }

  private static class TruncateToMonthTimestampNanoFunction extends NullSafeFunction {
    static final TruncateToMonthTimestampNanoFunction INSTANCE =
        new TruncateToMonthTimestampNanoFunction();

    @Override
    Object applyNonNull(Object value) {
      long nanos = (Long) value;
      LocalDateTime ldt = DateTimeUtil.timestampFromNanos(nanos);
      LocalDateTime truncated =
          ldt.withDayOfMonth(1).withHour(0).withMinute(0).withSecond(0).withNano(0);
      return DateTimeUtil.nanosFromTimestamp(truncated);
    }
  }

  private static class Sha256Function extends NullSafeFunction {
    private static final ThreadLocal<MessageDigest> DIGEST =
        ThreadLocal.withInitial(
            () -> {
              try {
                return MessageDigest.getInstance("SHA-256");
              } catch (NoSuchAlgorithmException e) {
                throw new IllegalStateException("SHA-256 not available", e);
              }
            });

    private final Type.TypeID typeId;
    private final byte[] salt;

    Sha256Function(Type.TypeID typeId, byte[] salt) {
      this.typeId = typeId;
      this.salt = salt != null ? salt.clone() : null;
    }

    @Override
    Object applyNonNull(Object value) {
      MessageDigest md = DIGEST.get();
      md.reset();
      if (salt != null) {
        md.update(salt);
      }
      updateCanonical(md, value);
      return encode(md.digest());
    }

    private void updateCanonical(MessageDigest md, Object value) {
      switch (typeId) {
        case STRING:
          md.update(value.toString().getBytes(StandardCharsets.UTF_8));
          break;
        case INTEGER:
          int intVal = (Integer) value;
          md.update((byte) intVal);
          md.update((byte) (intVal >>> 8));
          md.update((byte) (intVal >>> 16));
          md.update((byte) (intVal >>> 24));
          break;
        case LONG:
          long lv = (Long) value;
          for (int i = 0; i < 8; i++) {
            md.update((byte) lv);
            lv >>>= 8;
          }
          break;
        case BINARY:
          md.update(((ByteBuffer) value).duplicate());
          break;
        default:
          throw new UnsupportedOperationException("sha-256 is not supported for type: " + typeId);
      }
    }

    private Object encode(byte[] digest) {
      switch (typeId) {
        case STRING:
          return BaseEncoding.base16().lowerCase().encode(digest);
        case INTEGER:
          return ByteBuffer.wrap(digest, 0, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
        case LONG:
          return ByteBuffer.wrap(digest, 0, 8).order(ByteOrder.LITTLE_ENDIAN).getLong();
        case BINARY:
          return ByteBuffer.wrap(digest);
        default:
          throw new UnsupportedOperationException("sha-256 is not supported for type: " + typeId);
      }
    }
  }
}
