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
package org.apache.iceberg.parquet;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.ZoneId;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.spark.types.variant.Variant;
import org.apache.spark.types.variant.VariantBuilder;

public class ParquetVariant {
  private Variant internalVariant;

  public static ParquetVariant parseJson(String json) throws IOException {
    return new ParquetVariant(VariantBuilder.parseJson(json));
  }

  public static ParquetVariant of(ByteBuffer value, ByteBuffer metadata) {
    return new ParquetVariant(new Variant(value.array(), metadata.array()));
  }

  public static ParquetVariant toVariant(Type.PrimitiveType type, Object value) {
    VariantBuilder builder = new VariantBuilder();
    if (value == null) {
      builder.appendNull();
    } else {
      if (type instanceof Types.BooleanType) {
        builder.appendBoolean((boolean) value);
      } else if (type instanceof Types.IntegerType) {
        builder.appendLong((int) value);
      } else if (type instanceof Types.LongType) {
        builder.appendLong((long) value);
      } else if (type instanceof Types.FloatType) {
        builder.appendFloat((float) value);
      } else if (type instanceof Types.DoubleType) {
        builder.appendDouble((double) value);
      } else if (type instanceof Types.StringType) {
        builder.appendString((String) value);
      } else if (type instanceof Types.BinaryType || type instanceof Types.FixedType) {
        builder.appendBinary((byte[]) value);
      } else if (type instanceof Types.DecimalType) {
        builder.appendDecimal((BigDecimal) value);
      } else if (type instanceof Types.TimestampType) {
        if (((Types.TimestampType) type).shouldAdjustToUTC()) {
          builder.appendTimestamp((long) value);
        } else {
          builder.appendTimestampNtz((long) value);
        }
      } else if (type instanceof Types.DateType) {
        builder.appendDate((int) value);
      } else if (type instanceof Types.TimeType
          || type instanceof Types.TimestampNanoType
          || type instanceof Types.UUIDType) {
        throw new UnsupportedOperationException("Unsupported type in variant: " + type);
      }
    }

    return new ParquetVariant(builder.result());
  }

  private ParquetVariant(Variant internalVariant) {
    this.internalVariant = internalVariant;
  }

  public byte[] getValue() {
    return internalVariant.getValue();
  }

  public byte[] getMetadata() {
    return internalVariant.getMetadata();
  }

  public String toJson(ZoneId zoneId) {
    return internalVariant.toJson(zoneId);
  }
}
