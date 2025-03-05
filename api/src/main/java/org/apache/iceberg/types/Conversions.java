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
package org.apache.iceberg.types;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.UUID;
import org.apache.iceberg.Geography;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.util.UUIDUtil;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.CoordinateXYM;
import org.locationtech.jts.geom.CoordinateXYZM;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;

public class Conversions {

  private Conversions() {}

  private static final String HIVE_NULL = "__HIVE_DEFAULT_PARTITION__";

  private static final GeometryFactory FACTORY = new GeometryFactory();

  public static Object fromPartitionString(Type type, String asString) {
    if (asString == null || HIVE_NULL.equals(asString)) {
      return null;
    }

    switch (type.typeId()) {
      case BOOLEAN:
        return Boolean.valueOf(asString);
      case INTEGER:
        return Integer.valueOf(asString);
      case LONG:
        return Long.valueOf(asString);
      case FLOAT:
        return Float.valueOf(asString);
      case DOUBLE:
        return Double.valueOf(asString);
      case STRING:
        return asString;
      case UUID:
        return UUID.fromString(asString);
      case FIXED:
        Types.FixedType fixed = (Types.FixedType) type;
        return Arrays.copyOf(asString.getBytes(StandardCharsets.UTF_8), fixed.length());
      case BINARY:
        return asString.getBytes(StandardCharsets.UTF_8);
      case DECIMAL:
        return new BigDecimal(asString);
      case DATE:
        return Literal.of(asString).to(Types.DateType.get()).value();
      default:
        throw new UnsupportedOperationException(
            "Unsupported type for fromPartitionString: " + type);
    }
  }

  private static final ThreadLocal<CharsetEncoder> ENCODER =
      ThreadLocal.withInitial(StandardCharsets.UTF_8::newEncoder);
  private static final ThreadLocal<CharsetDecoder> DECODER =
      ThreadLocal.withInitial(StandardCharsets.UTF_8::newDecoder);

  public static ByteBuffer toByteBuffer(Type type, Object value) {
    return toByteBuffer(type.typeId(), value);
  }

  public static ByteBuffer toByteBuffer(Type.TypeID typeId, Object value) {
    if (value == null) {
      return null;
    }

    switch (typeId) {
      case BOOLEAN:
        return ByteBuffer.allocate(1).put(0, (Boolean) value ? (byte) 0x01 : (byte) 0x00);
      case INTEGER:
      case DATE:
        return ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(0, (int) value);
      case LONG:
      case TIME:
      case TIMESTAMP:
      case TIMESTAMP_NANO:
        return ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(0, (long) value);
      case FLOAT:
        return ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putFloat(0, (float) value);
      case DOUBLE:
        return ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putDouble(0, (double) value);
      case STRING:
        CharBuffer buffer = CharBuffer.wrap((CharSequence) value);
        try {
          return ENCODER.get().encode(buffer);
        } catch (CharacterCodingException e) {
          throw new RuntimeIOException(e, "Failed to encode value as UTF-8: %s", value);
        }
      case UUID:
        return UUIDUtil.convertToByteBuffer((UUID) value);
      case FIXED:
      case BINARY:
        return (ByteBuffer) value;
      case DECIMAL:
        return ByteBuffer.wrap(((BigDecimal) value).unscaledValue().toByteArray());
      case GEOMETRY:
        return geometryToByteBuffer((Geometry) value);
      case GEOGRAPHY:
        return geometryToByteBuffer(((Geography) value).geometry());
      default:
        throw new UnsupportedOperationException("Cannot serialize type: " + typeId);
    }
  }

  @SuppressWarnings("unchecked")
  public static <T> T fromByteBuffer(Type type, ByteBuffer buffer) {
    return (T) internalFromByteBuffer(type, buffer);
  }

  private static Object internalFromByteBuffer(Type type, ByteBuffer buffer) {
    if (buffer == null) {
      return null;
    }

    ByteBuffer tmp = buffer.duplicate();
    if (type == Types.UUIDType.get() || type instanceof Types.DecimalType) {
      tmp.order(ByteOrder.BIG_ENDIAN);
    } else {
      tmp.order(ByteOrder.LITTLE_ENDIAN);
    }
    switch (type.typeId()) {
      case BOOLEAN:
        return tmp.get() != 0x00;
      case INTEGER:
      case DATE:
        return tmp.getInt();
      case LONG:
      case TIME:
      case TIMESTAMP:
      case TIMESTAMP_NANO:
        if (tmp.remaining() < 8) {
          // type was later promoted to long
          return (long) tmp.getInt();
        }
        return tmp.getLong();
      case FLOAT:
        return tmp.getFloat();
      case DOUBLE:
        if (tmp.remaining() < 8) {
          // type was later promoted to long
          return (double) tmp.getFloat();
        }
        return tmp.getDouble();
      case STRING:
        try {
          return DECODER.get().decode(tmp);
        } catch (CharacterCodingException e) {
          throw new RuntimeIOException(e, "Failed to decode value as UTF-8: %s", buffer);
        }
      case UUID:
        return UUIDUtil.convert(tmp);
      case FIXED:
      case BINARY:
        return tmp;
      case DECIMAL:
        Types.DecimalType decimal = (Types.DecimalType) type;
        byte[] unscaledBytes = new byte[buffer.remaining()];
        tmp.get(unscaledBytes);
        return new BigDecimal(new BigInteger(unscaledBytes), decimal.scale());
      case GEOMETRY:
      case GEOGRAPHY:
        Coordinate coordinate = coordinateFromByteBuffer(tmp);
        Geometry geometry = FACTORY.createPoint(coordinate);
        if (type.typeId() == Type.TypeID.GEOMETRY) {
          return geometry;
        } else {
          return new Geography(geometry);
        }
      default:
        throw new UnsupportedOperationException("Cannot deserialize type: " + type);
    }
  }

  private static ByteBuffer geometryToByteBuffer(Geometry value) {
    if (value instanceof Point) {
      Coordinate coordinate = value.getCoordinate();
      return coordinateToByteBuffer(coordinate);
    } else {
      throw new IllegalArgumentException("Only point geometry can be converted to byte buffer");
    }
  }

  private static ByteBuffer coordinateToByteBuffer(Coordinate coordinate) {
    // The getZ() and getM() for a coordinate will return NaN if the value is not set.
    // This is conformant with the Bound Serialization spec.
    // See https://iceberg.apache.org/spec/#bound-serialization
    return ByteBuffer.allocate(32)
        .order(ByteOrder.LITTLE_ENDIAN)
        .putDouble(0, coordinate.getX())
        .putDouble(8, coordinate.getY())
        .putDouble(16, coordinate.getZ())
        .putDouble(24, coordinate.getM());
  }

  private static Coordinate coordinateFromByteBuffer(ByteBuffer tmp) {
    double coordX = tmp.getDouble(0);
    double coordY = tmp.getDouble(8);
    double coordZ = tmp.getDouble(16);
    double coordM = tmp.getDouble(24);
    boolean hasZ = !Double.isNaN(coordZ);
    boolean hasM = !Double.isNaN(coordM);
    Coordinate coordinate;
    if (hasZ && hasM) {
      coordinate = new CoordinateXYZM(coordX, coordY, coordZ, coordM);
    } else if (hasZ) {
      coordinate = new Coordinate(coordX, coordY, coordZ);
    } else if (hasM) {
      coordinate = new CoordinateXYM(coordX, coordY, coordM);
    } else {
      coordinate = new CoordinateXY(coordX, coordY);
    }
    return coordinate;
  }
}
