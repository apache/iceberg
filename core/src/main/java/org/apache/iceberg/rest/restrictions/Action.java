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

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.UUID;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.io.BaseEncoding;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.iceberg.util.SerializableFunction;

/**
 * A column projection action from the ReadRestrictions spec.
 *
 * <p>An {@code Action} is both a data carrier (wire-format discriminator + field id) and a factory
 * for the masking function that applies its semantics. {@link #bind(Type)} dispatches to the
 * type-specific {@link SerializableFunction} that implements the action. This mirrors the pattern
 * used by {@link org.apache.iceberg.transforms.Transform} in the partition-transform hierarchy.
 *
 * <p>All bound functions honor the spec invariant that null input produces null output.
 *
 * @param <S> source value type
 * @param <T> masked output type (usually equal to S; only {@link ApplyExpression} differs)
 */
public interface Action<S, T> extends Serializable {

  String MASK_ALPHANUM = "mask-alphanum";
  String MASK_TO_DEFAULT = "mask-to-default";
  String REPLACE_WITH_NULL = "replace-with-null";
  String SHOW_FIRST_4 = "show-first-4";
  String SHOW_LAST_4 = "show-last-4";
  String TRUNCATE_TO_YEAR = "truncate-to-year";
  String TRUNCATE_TO_MONTH = "truncate-to-month";
  String SHA_256_GLOBAL = "sha-256-global";
  String SHA_256_QUERY_LOCAL = "sha-256-query-local";
  String APPLY_EXPRESSION = "apply-expression";

  /** The action discriminator string as sent on the wire. */
  String actionType();

  /** The field id of the column this action applies to. */
  int fieldId();

  /**
   * Returns a function that applies this action to values of the given {@link Type}.
   *
   * @throws IllegalArgumentException if the type is not supported by this action.
   */
  default SerializableFunction<S, T> bind(Type type) {
    throw new UnsupportedOperationException("bind is not implemented for " + getClass().getName());
  }

  /**
   * Variant that accepts a per-query salt. Only {@link Sha256QueryLocal} uses the salt; other
   * actions ignore it and delegate to {@link #bind(Type)}.
   */
  default SerializableFunction<S, T> bind(Type type, byte[] salt) {
    return bind(type);
  }

  /** Returns true if this action can be bound to the given {@link Type}. */
  boolean canBind(Type type);

  /**
   * Base for all concrete actions. Holds the field id; subclasses carry any action-specific state
   * (expression, salt, etc.) and implement {@link #bind(Type)}/{@link #canBind(Type)}.
   */
  abstract class BaseAction<S, T> implements Action<S, T> {
    private final int fieldId;

    BaseAction(int fieldId) {
      this.fieldId = fieldId;
    }

    @Override
    public final int fieldId() {
      return fieldId;
    }
  }

  // ---------------------------------------------------------------------------
  // String masks (STRING -> STRING)
  // ---------------------------------------------------------------------------

  final class MaskAlphanum extends BaseAction<String, String> {
    public MaskAlphanum(int fieldId) {
      super(fieldId);
    }

    @Override
    public String actionType() {
      return MASK_ALPHANUM;
    }

    @Override
    public boolean canBind(Type type) {
      return type.typeId() == Type.TypeID.STRING;
    }

    @Override
    public SerializableFunction<String, String> bind(Type type) {
      Preconditions.checkArgument(
          canBind(type), "mask-alphanum requires STRING type, got %s", type);
      return MaskAlphanumFn.INSTANCE;
    }

    private static final class MaskAlphanumFn extends Actions.NullSafeFunction<String, String> {
      static final MaskAlphanumFn INSTANCE = new MaskAlphanumFn();

      @Override
      protected String applyNonNull(String input) {
        StringBuilder sb = new StringBuilder(input.length());
        int offset = 0;
        while (offset < input.length()) {
          int cp = input.codePointAt(offset);
          sb.appendCodePoint(Actions.mapCodePoint(cp));
          offset += Character.charCount(cp);
        }
        return sb.toString();
      }
    }
  }

  final class ShowFirst4 extends BaseAction<String, String> {
    public ShowFirst4(int fieldId) {
      super(fieldId);
    }

    @Override
    public String actionType() {
      return SHOW_FIRST_4;
    }

    @Override
    public boolean canBind(Type type) {
      return type.typeId() == Type.TypeID.STRING;
    }

    @Override
    public SerializableFunction<String, String> bind(Type type) {
      Preconditions.checkArgument(canBind(type), "show-first-4 requires STRING type, got %s", type);
      return ShowFirst4Fn.INSTANCE;
    }

    private static final class ShowFirst4Fn extends Actions.NullSafeFunction<String, String> {
      static final ShowFirst4Fn INSTANCE = new ShowFirst4Fn();

      @Override
      protected String applyNonNull(String input) {
        if (input.codePointCount(0, input.length()) <= 4) {
          return input;
        }
        StringBuilder sb = new StringBuilder(input.length());
        int cpIndex = 0;
        int offset = 0;
        while (offset < input.length()) {
          int cp = input.codePointAt(offset);
          sb.appendCodePoint(cpIndex < 4 ? cp : Actions.mapCodePoint(cp));
          offset += Character.charCount(cp);
          cpIndex++;
        }
        return sb.toString();
      }
    }
  }

  final class ShowLast4 extends BaseAction<String, String> {
    public ShowLast4(int fieldId) {
      super(fieldId);
    }

    @Override
    public String actionType() {
      return SHOW_LAST_4;
    }

    @Override
    public boolean canBind(Type type) {
      return type.typeId() == Type.TypeID.STRING;
    }

    @Override
    public SerializableFunction<String, String> bind(Type type) {
      Preconditions.checkArgument(canBind(type), "show-last-4 requires STRING type, got %s", type);
      return ShowLast4Fn.INSTANCE;
    }

    private static final class ShowLast4Fn extends Actions.NullSafeFunction<String, String> {
      static final ShowLast4Fn INSTANCE = new ShowLast4Fn();

      @Override
      protected String applyNonNull(String input) {
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
          sb.appendCodePoint(cpIndex >= keepFrom ? cp : Actions.mapCodePoint(cp));
          offset += Character.charCount(cp);
          cpIndex++;
        }
        return sb.toString();
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Polymorphic replacement masks
  // ---------------------------------------------------------------------------

  /** Returns null for every non-null input. Works for any type. */
  final class ReplaceWithNull extends BaseAction<Object, Object> {
    public ReplaceWithNull(int fieldId) {
      super(fieldId);
    }

    @Override
    public String actionType() {
      return REPLACE_WITH_NULL;
    }

    @Override
    public boolean canBind(Type type) {
      return true;
    }

    @Override
    public SerializableFunction<Object, Object> bind(Type type) {
      return ReplaceWithNullFn.INSTANCE;
    }

    private static final class ReplaceWithNullFn implements SerializableFunction<Object, Object> {
      static final ReplaceWithNullFn INSTANCE = new ReplaceWithNullFn();

      @Override
      public Object apply(Object value) {
        return null;
      }
    }
  }

  /** Returns a spec-defined default value for the column's type. */
  final class MaskToDefault extends BaseAction<Object, Object> {
    public MaskToDefault(int fieldId) {
      super(fieldId);
    }

    @Override
    public String actionType() {
      return MASK_TO_DEFAULT;
    }

    @Override
    public boolean canBind(Type type) {
      switch (type.typeId()) {
        case BOOLEAN:
        case INTEGER:
        case LONG:
        case FLOAT:
        case DOUBLE:
        case STRING:
        case DATE:
        case TIME:
        case TIMESTAMP:
        case TIMESTAMP_NANO:
        case UUID:
        case FIXED:
        case BINARY:
        case DECIMAL:
          return true;
        default:
          return false;
      }
    }

    @Override
    public SerializableFunction<Object, Object> bind(Type type) {
      Preconditions.checkArgument(
          canBind(type), "mask-to-default is not supported for type: %s", type);
      Object defaultValue = defaultValueFor(type);
      return defaultValue instanceof ByteBuffer
          ? new ConstantByteBufferFn((ByteBuffer) defaultValue)
          : new ConstantFn(defaultValue);
    }

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
          return ByteBuffer.allocate(((Types.FixedType) type).length()).asReadOnlyBuffer();
        case BINARY:
          return EMPTY_BUFFER;
        case DECIMAL:
          return new BigDecimal(BigInteger.ZERO, ((Types.DecimalType) type).scale());
        default:
          throw new IllegalStateException("unreachable: canBind should have rejected " + type);
      }
    }

    private static final Integer INT_DEFAULT = 999999999;
    private static final Long LONG_DEFAULT = 999999999L;
    private static final Float FLOAT_DEFAULT = 0.0f;
    private static final Double DOUBLE_DEFAULT = 0.0d;
    private static final String STRING_DEFAULT = "XXXXXXXX";
    private static final Integer DATE_DEFAULT =
        DateTimeUtil.daysFromDate(LocalDate.of(9999, 12, 31));
    private static final Long TIME_DEFAULT_MICROS = 0L;
    private static final Long TIMESTAMP_DEFAULT_MICROS =
        DateTimeUtil.microsFromTimestamp(LocalDateTime.of(9999, 12, 31, 0, 0));
    // Per spec: nanos types use 2261-12-31 because 9999 overflows Long nanoseconds from epoch.
    private static final Long TIMESTAMP_DEFAULT_NANOS =
        DateTimeUtil.nanosFromTimestamp(LocalDateTime.of(2261, 12, 31, 0, 0));
    private static final UUID UUID_DEFAULT =
        UUID.fromString("00000000-0000-0000-0000-000000000000");
    private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0).asReadOnlyBuffer();

    private static final class ConstantFn extends Actions.NullSafeFunction<Object, Object> {
      private final Object constant;

      ConstantFn(Object constant) {
        this.constant = constant;
      }

      @Override
      protected Object applyNonNull(Object value) {
        return constant;
      }
    }

    // ByteBuffer has a mutable position; returning a fresh duplicate per call keeps callers
    // isolated from each other.
    private static final class ConstantByteBufferFn
        extends Actions.NullSafeFunction<Object, Object> {
      private final ByteBuffer constant;

      ConstantByteBufferFn(ByteBuffer constant) {
        this.constant = constant;
      }

      @Override
      protected Object applyNonNull(Object value) {
        return constant.duplicate();
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Truncation (DATE / TIMESTAMP / TIMESTAMP_NANO -> same)
  // ---------------------------------------------------------------------------

  final class TruncateToYear extends BaseAction<Object, Object> {
    public TruncateToYear(int fieldId) {
      super(fieldId);
    }

    @Override
    public String actionType() {
      return TRUNCATE_TO_YEAR;
    }

    @Override
    public boolean canBind(Type type) {
      switch (type.typeId()) {
        case DATE:
        case TIMESTAMP:
        case TIMESTAMP_NANO:
          return true;
        default:
          return false;
      }
    }

    @Override
    public SerializableFunction<Object, Object> bind(Type type) {
      Preconditions.checkArgument(
          canBind(type), "truncate-to-year is not supported for type: %s", type);
      switch (type.typeId()) {
        case DATE:
          return TruncateDateFn.YEAR;
        case TIMESTAMP:
          return TruncateTimestampFn.YEAR;
        case TIMESTAMP_NANO:
          return TruncateTimestampNanoFn.YEAR;
        default:
          throw new IllegalStateException("unreachable");
      }
    }
  }

  final class TruncateToMonth extends BaseAction<Object, Object> {
    public TruncateToMonth(int fieldId) {
      super(fieldId);
    }

    @Override
    public String actionType() {
      return TRUNCATE_TO_MONTH;
    }

    @Override
    public boolean canBind(Type type) {
      switch (type.typeId()) {
        case DATE:
        case TIMESTAMP:
        case TIMESTAMP_NANO:
          return true;
        default:
          return false;
      }
    }

    @Override
    public SerializableFunction<Object, Object> bind(Type type) {
      Preconditions.checkArgument(
          canBind(type), "truncate-to-month is not supported for type: %s", type);
      switch (type.typeId()) {
        case DATE:
          return TruncateDateFn.MONTH;
        case TIMESTAMP:
          return TruncateTimestampFn.MONTH;
        case TIMESTAMP_NANO:
          return TruncateTimestampNanoFn.MONTH;
        default:
          throw new IllegalStateException("unreachable");
      }
    }
  }

  /** Truncates epoch-days to the first day of year or month. */
  abstract class TruncateDateFn extends Actions.NullSafeFunction<Object, Object> {
    static final TruncateDateFn YEAR =
        new TruncateDateFn() {
          @Override
          LocalDate truncate(LocalDate date) {
            return date.withMonth(1).withDayOfMonth(1);
          }
        };

    static final TruncateDateFn MONTH =
        new TruncateDateFn() {
          @Override
          LocalDate truncate(LocalDate date) {
            return date.withDayOfMonth(1);
          }
        };

    @Override
    protected Object applyNonNull(Object value) {
      return DateTimeUtil.daysFromDate(truncate(DateTimeUtil.dateFromDays((Integer) value)));
    }

    abstract LocalDate truncate(LocalDate date);
  }

  /** Truncates micros-since-epoch timestamps to the first instant of year or month. */
  abstract class TruncateTimestampFn extends Actions.NullSafeFunction<Object, Object> {
    static final TruncateTimestampFn YEAR =
        new TruncateTimestampFn() {
          @Override
          LocalDateTime truncate(LocalDateTime ts) {
            return ts.withMonth(1).withDayOfMonth(1).with(LocalTime.MIDNIGHT);
          }
        };

    static final TruncateTimestampFn MONTH =
        new TruncateTimestampFn() {
          @Override
          LocalDateTime truncate(LocalDateTime ts) {
            return ts.withDayOfMonth(1).with(LocalTime.MIDNIGHT);
          }
        };

    @Override
    protected Object applyNonNull(Object value) {
      return DateTimeUtil.microsFromTimestamp(
          truncate(DateTimeUtil.timestampFromMicros((Long) value)));
    }

    abstract LocalDateTime truncate(LocalDateTime ts);
  }

  /** Truncates nanos-since-epoch timestamps to the first instant of year or month. */
  abstract class TruncateTimestampNanoFn extends Actions.NullSafeFunction<Object, Object> {
    static final TruncateTimestampNanoFn YEAR =
        new TruncateTimestampNanoFn() {
          @Override
          LocalDateTime truncate(LocalDateTime ts) {
            return ts.withMonth(1).withDayOfMonth(1).with(LocalTime.MIDNIGHT);
          }
        };

    static final TruncateTimestampNanoFn MONTH =
        new TruncateTimestampNanoFn() {
          @Override
          LocalDateTime truncate(LocalDateTime ts) {
            return ts.withDayOfMonth(1).with(LocalTime.MIDNIGHT);
          }
        };

    @Override
    protected Object applyNonNull(Object value) {
      return DateTimeUtil.nanosFromTimestamp(
          truncate(DateTimeUtil.timestampFromNanos((Long) value)));
    }

    abstract LocalDateTime truncate(LocalDateTime ts);
  }

  // ---------------------------------------------------------------------------
  // SHA-256 (STRING / INTEGER / LONG / BINARY -> same)
  // ---------------------------------------------------------------------------

  final class Sha256Global extends BaseAction<Object, Object> {
    public Sha256Global(int fieldId) {
      super(fieldId);
    }

    @Override
    public String actionType() {
      return SHA_256_GLOBAL;
    }

    @Override
    public boolean canBind(Type type) {
      return Sha256.isSupported(type);
    }

    @Override
    public SerializableFunction<Object, Object> bind(Type type) {
      Preconditions.checkArgument(canBind(type), "sha-256 is not supported for type: %s", type);
      return Sha256.forType(type.typeId(), null);
    }
  }

  final class Sha256QueryLocal extends BaseAction<Object, Object> {
    public Sha256QueryLocal(int fieldId) {
      super(fieldId);
    }

    @Override
    public String actionType() {
      return SHA_256_QUERY_LOCAL;
    }

    @Override
    public boolean canBind(Type type) {
      return Sha256.isSupported(type);
    }

    @Override
    public SerializableFunction<Object, Object> bind(Type type) {
      throw new IllegalArgumentException(
          "sha-256-query-local requires a salt; call bind(Type, byte[]) instead");
    }

    @Override
    public SerializableFunction<Object, Object> bind(Type type, byte[] salt) {
      Preconditions.checkArgument(canBind(type), "sha-256 is not supported for type: %s", type);
      Preconditions.checkArgument(
          salt != null && salt.length >= 16, "sha-256-query-local salt must be >= 16 bytes");
      return Sha256.forType(type.typeId(), salt);
    }
  }

  /** Shared SHA-256 function set; one subclass per supported input type. */
  final class Sha256 {
    private static final ThreadLocal<MessageDigest> DIGEST =
        ThreadLocal.withInitial(
            () -> {
              try {
                return MessageDigest.getInstance("SHA-256");
              } catch (java.security.NoSuchAlgorithmException e) {
                throw new IllegalStateException("SHA-256 not available", e);
              }
            });

    private Sha256() {}

    static boolean isSupported(Type type) {
      switch (type.typeId()) {
        case STRING:
        case INTEGER:
        case LONG:
        case BINARY:
          return true;
        default:
          return false;
      }
    }

    static SerializableFunction<Object, Object> forType(Type.TypeID typeId, byte[] salt) {
      switch (typeId) {
        case STRING:
          return new Sha256String(salt);
        case INTEGER:
          return new Sha256Integer(salt);
        case LONG:
          return new Sha256Long(salt);
        case BINARY:
          return new Sha256Binary(salt);
        default:
          throw new IllegalStateException("unreachable: " + typeId);
      }
    }

    abstract static class Base extends Actions.NullSafeFunction<Object, Object> {
      private final byte[] salt;

      Base(byte[] salt) {
        this.salt = salt != null ? salt.clone() : null;
      }

      @Override
      protected final Object applyNonNull(Object value) {
        MessageDigest md = DIGEST.get();
        md.reset();
        if (salt != null) {
          md.update(salt);
        }
        update(md, value);
        return encode(md.digest());
      }

      abstract void update(MessageDigest md, Object value);

      abstract Object encode(byte[] digest);
    }

    private static final class Sha256String extends Base {
      Sha256String(byte[] salt) {
        super(salt);
      }

      @Override
      void update(MessageDigest md, Object value) {
        md.update(((String) value).getBytes(StandardCharsets.UTF_8));
      }

      @Override
      Object encode(byte[] digest) {
        return BaseEncoding.base16().lowerCase().encode(digest);
      }
    }

    private static final class Sha256Integer extends Base {
      Sha256Integer(byte[] salt) {
        super(salt);
      }

      @Override
      void update(MessageDigest md, Object value) {
        int intVal = (Integer) value;
        md.update((byte) intVal);
        md.update((byte) (intVal >>> 8));
        md.update((byte) (intVal >>> 16));
        md.update((byte) (intVal >>> 24));
      }

      @Override
      Object encode(byte[] digest) {
        return ByteBuffer.wrap(digest, 0, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
      }
    }

    private static final class Sha256Long extends Base {
      Sha256Long(byte[] salt) {
        super(salt);
      }

      @Override
      void update(MessageDigest md, Object value) {
        long lv = (Long) value;
        for (int i = 0; i < 8; i++) {
          md.update((byte) lv);
          lv >>>= 8;
        }
      }

      @Override
      Object encode(byte[] digest) {
        return ByteBuffer.wrap(digest, 0, 8).order(ByteOrder.LITTLE_ENDIAN).getLong();
      }
    }

    private static final class Sha256Binary extends Base {
      Sha256Binary(byte[] salt) {
        super(salt);
      }

      @Override
      void update(MessageDigest md, Object value) {
        md.update(((ByteBuffer) value).duplicate());
      }

      @Override
      Object encode(byte[] digest) {
        return ByteBuffer.wrap(digest);
      }
    }
  }

  // ---------------------------------------------------------------------------
  // apply-expression (not supported by this client)
  // ---------------------------------------------------------------------------

  final class ApplyExpression extends BaseAction<Object, Object> {
    private final Expression expression;

    public ApplyExpression(int fieldId, Expression expression) {
      super(fieldId);
      Preconditions.checkArgument(expression != null, "Invalid expression: null");
      this.expression = expression;
    }

    public Expression expression() {
      return expression;
    }

    @Override
    public String actionType() {
      return APPLY_EXPRESSION;
    }

    @Override
    public boolean canBind(Type type) {
      // bind() always succeeds (returns a function that throws on apply); this reflects that.
      return true;
    }

    @Override
    public SerializableFunction<Object, Object> bind(Type type) {
      return ApplyExpressionFn.INSTANCE;
    }

    private static final class ApplyExpressionFn implements SerializableFunction<Object, Object> {
      static final ApplyExpressionFn INSTANCE = new ApplyExpressionFn();

      @Override
      public Object apply(Object value) {
        throw new UnsupportedOperationException(
            "apply-expression column projection is not supported by this client "
                + "(Iceberg Expression is currently boolean-only)");
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Unknown action (forward-compat)
  // ---------------------------------------------------------------------------

  /**
   * Preserves an action with a discriminator string this client doesn't recognize so that newer
   * server-side action types don't break parsing. Callers that intend to enforce the action
   * (engine-side rules) must fail closed when they encounter this — silent skipping would leak
   * unmasked data.
   */
  final class Unknown extends BaseAction<Object, Object> {
    private final String actionType;

    public Unknown(int fieldId, String actionType) {
      super(fieldId);
      Preconditions.checkArgument(actionType != null, "Invalid action type: null");
      this.actionType = actionType;
    }

    @Override
    public String actionType() {
      return actionType;
    }

    @Override
    public boolean canBind(Type type) {
      return false;
    }

    @Override
    public SerializableFunction<Object, Object> bind(Type type) {
      throw new IllegalStateException(
          "Cannot bind unknown action type '"
              + actionType
              + "': this client does not recognize the action. Upgrade the client or remove the "
              + "action from the server-side policy.");
    }
  }
}
