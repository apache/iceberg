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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigInteger;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.types.Types.TimestampNanoType;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;

class TestParquetConversions {

  @Test
  void int96NanosecondConverterPreservesTargetUnit() {
    PrimitiveType int96 = Types.optional(PrimitiveTypeName.INT96).named("ts");
    assertThat(
            ParquetConversions.converterFromParquet(int96, TimestampNanoType.withZone())
                .apply(Int96TestUtil.encode(BigInteger.valueOf(1001))))
        .isEqualTo(1001L);
  }

  @Test
  void int96MicrosecondConverterTruncatesPrecision() {
    PrimitiveType int96 = Types.optional(PrimitiveTypeName.INT96).named("ts");
    for (long nanos : new long[] {-1001, -1, 1, 1001}) {
      assertThat(
              ParquetConversions.converterFromParquet(int96)
                  .apply(Int96TestUtil.encode(BigInteger.valueOf(nanos))))
          .isEqualTo(Math.floorDiv(nanos, 1000));
    }
  }

  @Test
  void int96NanosecondConverterRejectsOverflow() {
    PrimitiveType int96 = Types.optional(PrimitiveTypeName.INT96).named("ts");
    Binary value = Int96TestUtil.encode(BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE));
    assertThatThrownBy(
            () ->
                ParquetConversions.converterFromParquet(int96, TimestampNanoType.withZone())
                    .apply(value))
        .isInstanceOf(ArithmeticException.class)
        .hasMessageContaining("overflow");
  }

  private static PrimitiveType binaryWith(LogicalTypeAnnotation annotation) {
    return Types.required(PrimitiveTypeName.BINARY).as(annotation).named("s");
  }

  @Test
  void stringConverterDecodesUtf8EnumAndJson() {
    Binary value = Binary.fromString("hello");

    for (LogicalTypeAnnotation annotation :
        new LogicalTypeAnnotation[] {
          LogicalTypeAnnotation.stringType(),
          LogicalTypeAnnotation.enumType(),
          LogicalTypeAnnotation.jsonType()
        }) {
      Object converted =
          ParquetConversions.converterFromParquet(binaryWith(annotation), StringType.get())
              .apply(value);

      assertThat(converted)
          .as("%s should decode to a CharSequence", annotation)
          .isInstanceOf(CharSequence.class);
      assertThat(converted.toString()).isEqualTo("hello");
    }
  }
}
