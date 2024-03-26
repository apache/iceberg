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

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestIdentity {
  @Test
  public void testNullHumanString() {
    Types.LongType longType = Types.LongType.get();
    Transform<Long, Long> identity = Transforms.identity();

    assertThat(identity.toHumanString(longType, null))
        .as("Should produce \"null\" for null")
        .isEqualTo("null");
  }

  @Test
  public void testBinaryHumanString() {
    Types.BinaryType binary = Types.BinaryType.get();
    Transform<ByteBuffer, ByteBuffer> identity = Transforms.identity();

    assertThat(identity.toHumanString(binary, ByteBuffer.wrap(new byte[] {1, 2, 3})))
        .as("Should base64-encode binary")
        .isEqualTo("AQID");
  }

  @Test
  public void testFixedHumanString() {
    Types.FixedType fixed3 = Types.FixedType.ofLength(3);
    Transform<byte[], byte[]> identity = Transforms.identity();

    assertThat(identity.toHumanString(fixed3, new byte[] {1, 2, 3}))
        .as("Should base64-encode binary")
        .isEqualTo("AQID");
  }

  @Test
  public void testDateHumanString() {
    Types.DateType date = Types.DateType.get();
    Transform<Integer, Integer> identity = Transforms.identity();

    String dateString = "2017-12-01";
    Literal<Integer> dateLit = Literal.of(dateString).to(date);

    assertThat(identity.toHumanString(date, dateLit.value()))
        .as("Should produce identical date")
        .isEqualTo(dateString);
  }

  @Test
  public void testDateHumanStringDeprecated() {
    Types.DateType date = Types.DateType.get();
    Transform<Integer, Integer> identity = Transforms.identity(date);

    String dateString = "2017-12-01";
    Literal<Integer> dateLit = Literal.of(dateString).to(date);

    assertThat(identity.toHumanString(dateLit.value()))
        .as("Should produce identical date")
        .isEqualTo(dateString);
  }

  @Test
  public void testTimeHumanString() {
    Types.TimeType time = Types.TimeType.get();
    Transform<Long, Long> identity = Transforms.identity();

    String timeString = "10:12:55.038194";
    Literal<Long> timeLit = Literal.of(timeString).to(time);

    assertThat(identity.toHumanString(time, timeLit.value()))
        .as("Should produce identical time")
        .isEqualTo(timeString);
  }

  @Test
  public void testTimestampWithZoneHumanString() {
    Types.TimestampType timestamptz = Types.TimestampType.withZone();
    Transform<Long, Long> identity = Transforms.identity();

    Literal<Long> ts = Literal.of("2017-12-01T10:12:55.038194-08:00").to(timestamptz);

    // value will always be in UTC
    assertThat(identity.toHumanString(timestamptz, ts.value()))
        .as("Should produce timestamp with time zone adjusted to UTC")
        .isEqualTo("2017-12-01T18:12:55.038194Z");
  }

  @Test
  public void testTimestampWithoutZoneHumanString() {
    Types.TimestampType timestamp = Types.TimestampType.withoutZone();
    Transform<Long, Long> identity = Transforms.identity();

    String tsString = "2017-12-01T10:12:55.038194";
    Literal<Long> ts = Literal.of(tsString).to(timestamp);

    // value is not changed
    assertThat(identity.toHumanString(timestamp, ts.value()))
        .as("Should produce identical timestamp without time zone")
        .isEqualTo(tsString);
  }

  @Test
  public void testLongToHumanString() {
    Types.LongType longType = Types.LongType.get();
    Transform<Long, Long> identity = Transforms.identity();

    assertThat(identity.toHumanString(longType, -1234567890000L))
        .as("Should use Long toString")
        .isEqualTo("-1234567890000");
  }

  @Test
  public void testStringToHumanString() {
    Types.StringType string = Types.StringType.get();
    Transform<String, String> identity = Transforms.identity();

    String withSlash = "a/b/c=d";
    assertThat(identity.toHumanString(string, withSlash))
        .as("Should not modify Strings")
        .isEqualTo(withSlash);
  }

  @Test
  public void testBigDecimalToHumanString() {
    Types.DecimalType decimal = Types.DecimalType.of(9, 2);
    Transform<BigDecimal, BigDecimal> identity = Transforms.identity();

    String decimalString = "-1.50";
    BigDecimal bigDecimal = new BigDecimal(decimalString);
    assertThat(identity.toHumanString(decimal, bigDecimal))
        .as("Should not modify Strings")
        .isEqualTo(decimalString);
  }
}
