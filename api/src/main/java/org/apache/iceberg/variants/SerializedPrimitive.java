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
package org.apache.iceberg.variants;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.UUIDUtil;

class SerializedPrimitive implements VariantPrimitive<Object>, SerializedValue {
  private static final int PRIMITIVE_TYPE_SHIFT = 2;
  private static final int PRIMITIVE_OFFSET = 1;

  static SerializedPrimitive from(byte[] bytes) {
    return from(ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN), bytes[0]);
  }

  static SerializedPrimitive from(ByteBuffer value, int header) {
    Preconditions.checkArgument(
        value.order() == ByteOrder.LITTLE_ENDIAN, "Unsupported byte order: big endian");
    BasicType basicType = VariantUtil.basicType(header);
    Preconditions.checkArgument(
        basicType == BasicType.PRIMITIVE,
        "Invalid primitive, basic type != PRIMITIVE: " + basicType);
    return new SerializedPrimitive(value, header);
  }

  private final ByteBuffer value;
  private final PhysicalType type;
  private Object primitive = null;

  private SerializedPrimitive(ByteBuffer value, int header) {
    this.value = value;
    this.type = PhysicalType.from(header >> PRIMITIVE_TYPE_SHIFT);
  }

  private Object read() {
    switch (type) {
      case NULL:
        return null;
      case BOOLEAN_TRUE:
        return true;
      case BOOLEAN_FALSE:
        return false;
      case INT8:
        return VariantUtil.readLittleEndianInt8(value, PRIMITIVE_OFFSET);
      case INT16:
        return VariantUtil.readLittleEndianInt16(value, PRIMITIVE_OFFSET);
      case INT32:
      case DATE:
        return VariantUtil.readLittleEndianInt32(value, PRIMITIVE_OFFSET);
      case INT64:
      case TIMESTAMPTZ:
      case TIMESTAMPNTZ:
      case TIME:
      case TIMESTAMPTZ_NANOS:
      case TIMESTAMPNTZ_NANOS:
        return VariantUtil.readLittleEndianInt64(value, PRIMITIVE_OFFSET);
      case FLOAT:
        return VariantUtil.readFloat(value, PRIMITIVE_OFFSET);
      case DOUBLE:
        return VariantUtil.readDouble(value, PRIMITIVE_OFFSET);
      case DECIMAL4:
        {
          int scale = VariantUtil.readByte(value, PRIMITIVE_OFFSET);
          int unscaled = VariantUtil.readLittleEndianInt32(value, PRIMITIVE_OFFSET + 1);
          return new BigDecimal(BigInteger.valueOf(unscaled), scale);
        }
      case DECIMAL8:
        {
          int scale = VariantUtil.readByte(value, PRIMITIVE_OFFSET);
          long unscaled = VariantUtil.readLittleEndianInt64(value, PRIMITIVE_OFFSET + 1);
          return new BigDecimal(BigInteger.valueOf(unscaled), scale);
        }
      case DECIMAL16:
        {
          int scale = VariantUtil.readByte(value, PRIMITIVE_OFFSET);
          byte[] unscaled = new byte[16];
          for (int i = 0; i < 16; i += 1) {
            unscaled[i] = (byte) VariantUtil.readByte(value, PRIMITIVE_OFFSET + 16 - i);
          }
          return new BigDecimal(new BigInteger(unscaled), scale);
        }
      case BINARY:
        {
          int size = VariantUtil.readLittleEndianInt32(value, PRIMITIVE_OFFSET);
          return VariantUtil.slice(value, PRIMITIVE_OFFSET + 4, size);
        }
      case STRING:
        {
          int size = VariantUtil.readLittleEndianInt32(value, PRIMITIVE_OFFSET);
          return VariantUtil.readString(value, PRIMITIVE_OFFSET + 4, size);
        }
      case UUID:
        return UUIDUtil.convert(
            VariantUtil.slice(value, PRIMITIVE_OFFSET, 16).order(ByteOrder.BIG_ENDIAN));
    }

    throw new UnsupportedOperationException("Unsupported primitive type: " + type);
  }

  @Override
  public PhysicalType type() {
    return type;
  }

  @Override
  public Object get() {
    if (null == primitive) {
      this.primitive = read();
    }
    return primitive;
  }

  @Override
  public ByteBuffer buffer() {
    return value;
  }

  @Override
  public int hashCode() {
    return VariantPrimitive.hash(this);
  }

  @Override
  public boolean equals(Object other) {
    return VariantPrimitive.equals(this, other);
  }

  @Override
  public String toString() {
    return VariantPrimitive.asString(this);
  }
}
