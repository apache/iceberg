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
package org.apache.iceberg.spark;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.List;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.spark.sql.catalyst.expressions.EqualTo;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

public class TestSparkUtil {

  private static final StructType TIMESTAMP_SCHEMA =
      new StructType(
          new StructField[] {
            new StructField("ts", DataTypes.TimestampType, true, Metadata.empty())
          });

  @Test
  public void partitionMapToExpressionWithOffset() {
    // an explicit, non-UTC offset must be respected regardless of the JVM default zone, matching
    // the previous Joda-based behavior
    long expectedMicros =
        OffsetDateTime.parse("2021-01-01T12:34:56+05:00").toInstant().toEpochMilli() * 1000;
    assertThat(timestampLiteralMicros("2021-01-01T12:34:56+05:00")).isEqualTo(expectedMicros);
  }

  @Test
  public void partitionMapToExpressionWithUtcOffset() {
    long expectedMicros =
        OffsetDateTime.parse("2021-01-01T12:34:56Z").toInstant().toEpochMilli() * 1000;
    assertThat(timestampLiteralMicros("2021-01-01T12:34:56Z")).isEqualTo(expectedMicros);
  }

  @Test
  public void partitionMapToExpressionWithoutOffset() {
    // without an offset the value is interpreted in the system default zone
    long expectedMicros =
        LocalDateTime.parse("2021-01-01T12:34:56")
                .atZone(ZoneId.systemDefault())
                .toInstant()
                .toEpochMilli()
            * 1000;
    assertThat(timestampLiteralMicros("2021-01-01T12:34:56")).isEqualTo(expectedMicros);
  }

  private static long timestampLiteralMicros(String value) {
    List<Expression> expressions =
        SparkUtil.partitionMapToExpression(TIMESTAMP_SCHEMA, ImmutableMap.of("ts", value));
    assertThat(expressions).hasSize(1);
    Literal literal = (Literal) ((EqualTo) expressions.get(0)).right();
    // Spark stores TIMESTAMP literals as microseconds since the epoch
    return (Long) literal.value();
  }
}
