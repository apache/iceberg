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

package com.netflix.iceberg.parquet;

import com.netflix.iceberg.expressions.Literal;
import com.netflix.iceberg.types.Type;
import com.netflix.iceberg.types.Types;
import org.apache.commons.io.Charsets;
import org.apache.parquet.io.api.Binary;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.UUID;

class ParquetConversions {
  private ParquetConversions() {
  }

  static <T> Literal<T> fromParquetPrimitive(Type type, Object value) {
    if (value instanceof Boolean) {
      return Literal.of((Boolean) value).to(type);
    } else if (value instanceof Integer) {
      return Literal.of((Integer) value).to(type);
    } else if (value instanceof Long) {
      return Literal.of((Long) value).to(type);
    } else if (value instanceof Float) {
      return Literal.of((Float) value).to(type);
    } else if (value instanceof Double) {
      return Literal.of((Double) value).to(type);
    } else if (value instanceof Binary) {
      switch (type.typeId()) {
        case STRING:
          return Literal.of(Charsets.UTF_8.decode(((Binary) value).toByteBuffer())).to(type);
        case UUID:
          ByteBuffer buffer = ((Binary) value).toByteBuffer().order(ByteOrder.BIG_ENDIAN);
          long mostSigBits = buffer.getLong();
          long leastSigBits = buffer.getLong();
          return Literal.of(new UUID(mostSigBits, leastSigBits)).to(type);
        case FIXED:
        case BINARY:
          return Literal.of(((Binary) value).toByteBuffer()).to(type);
        case DECIMAL:
          Types.DecimalType decimal = (Types.DecimalType) type;
          return Literal.of(
              new BigDecimal(new BigInteger(((Binary) value).getBytes()), decimal.scale())
          ).to(type);
        default:
          throw new IllegalArgumentException("Unsupported primitive type: " + type);
      }
    } else {
      throw new IllegalArgumentException("Unsupported primitive value: " + value);
    }
  }
}
