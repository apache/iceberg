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
package org.apache.iceberg.functions;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.UUID;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.iceberg.util.SerializableFunction;
import org.apache.iceberg.variants.Variant;

/** Returns a spec-defined fixed value for the column's type. */
public final class MaskToFixedValue extends IcebergFunction.BaseFunction<Object, Object> {

  private static final Integer INT_DEFAULT = 0;
  private static final Long LONG_DEFAULT = 0L;
  private static final Float FLOAT_DEFAULT = 0.0f;
  private static final Double DOUBLE_DEFAULT = 0.0d;
  // Per spec: string uses "XXXXXXXX" (not "") so masked strings stay visually distinct from
  // legitimately empty strings. All other types use the zero value of their representation.
  private static final String STRING_DEFAULT = "XXXXXXXX";
  private static final Integer DATE_DEFAULT = DateTimeUtil.daysFromDate(LocalDate.of(1970, 1, 1));
  private static final Long TIME_DEFAULT_MICROS = 0L;
  private static final Long TIMESTAMP_DEFAULT_MICROS =
      DateTimeUtil.microsFromTimestamp(LocalDateTime.of(1970, 1, 1, 0, 0));
  private static final Long TIMESTAMP_DEFAULT_NANOS =
      DateTimeUtil.nanosFromTimestamp(LocalDateTime.of(1970, 1, 1, 0, 0));
  private static final UUID UUID_DEFAULT = UUID.fromString("00000000-0000-0000-0000-000000000000");
  private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0).asReadOnlyBuffer();

  // WKB POINT EMPTY: byte order (little-endian) | type=POINT | X=NaN | Y=NaN
  private static final ByteBuffer POINT_EMPTY =
      ByteBuffer.wrap(
              new byte[] {
                0x01, 0x01, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, (byte) 0xF8, 0x7F,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, (byte) 0xF8, 0x7F
              })
          .asReadOnlyBuffer();

  // Empty Variant: V1 metadata with no entries + empty object value.
  private static final Variant EMPTY_VARIANT =
      Variant.from(ByteBuffer.wrap(new byte[] {0x01, 0x00, 0x00, 0x02, 0x00, 0x00}));

  public MaskToFixedValue(int fieldId) {
    super(fieldId);
  }

  @Override
  public String name() {
    return MASK_TO_FIXED_VALUE;
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
      case VARIANT:
      case GEOMETRY:
      case GEOGRAPHY:
      case LIST:
      case MAP:
        return true;
      // TODO: support STRUCT (recursive type-specific defaults). Tracked as follow-up.
      default:
        return false;
    }
  }

  @Override
  public SerializableFunction<Object, Object> bind(Type type) {
    Preconditions.checkArgument(
        canBind(type), "mask-to-fixed-value is not supported for type: %s", type);
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
      case VARIANT:
        return EMPTY_VARIANT;
      case GEOMETRY:
      case GEOGRAPHY:
        return POINT_EMPTY;
      case LIST:
        return Collections.emptyList();
      case MAP:
        return Collections.emptyMap();
      default:
        throw new IllegalStateException("unreachable: canBind should have rejected " + type);
    }
  }

  private static final class ConstantFn implements SerializableFunction<Object, Object> {
    private final Object constant;

    ConstantFn(Object constant) {
      this.constant = constant;
    }

    @Override
    public Object apply(Object value) {
      return constant;
    }
  }

  private static final class ConstantByteBufferFn implements SerializableFunction<Object, Object> {
    private final ByteBuffer constant;

    ConstantByteBufferFn(ByteBuffer constant) {
      this.constant = constant;
    }

    @Override
    public Object apply(Object value) {
      return constant.duplicate();
    }
  }
}
