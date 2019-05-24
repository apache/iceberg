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

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

public class TestIdentity {
  @Test
  public void testNullHumanString() {
    Types.LongType longType = Types.LongType.get();
    Transform<Long, Long> identity = Transforms.identity(longType);

    Assert.assertEquals("Should produce \"null\" for null",
        "null", identity.toHumanString(null));
  }

  @Test
  public void testBinaryHumanString() {
    Types.BinaryType binary = Types.BinaryType.get();
    Transform<ByteBuffer, ByteBuffer> identity = Transforms.identity(binary);

    Assert.assertEquals("Should base64-encode binary",
        "AQID", identity.toHumanString(ByteBuffer.wrap(new byte[] {1, 2, 3})));
  }

  @Test
  public void testFixedHumanString() {
    Types.FixedType fixed3 = Types.FixedType.ofLength(3);
    Transform<byte[], byte[]> identity = Transforms.identity(fixed3);

    Assert.assertEquals("Should base64-encode binary",
        "AQID", identity.toHumanString(new byte[] {1, 2, 3}));
  }

  @Test
  public void testDateHumanString() {
    Types.DateType date = Types.DateType.get();
    Transform<Integer, Integer> identity = Transforms.identity(date);

    String dateString = "2017-12-01";
    Literal<Integer> dateLit = Literal.of(dateString).to(date);

    Assert.assertEquals("Should produce identical date",
        dateString, identity.toHumanString(dateLit.value()));
  }

  @Test
  public void testTimeHumanString() {
    Types.TimeType time = Types.TimeType.get();
    Transform<Long, Long> identity = Transforms.identity(time);

    String timeString = "10:12:55.038194";
    Literal<Long> timeLit = Literal.of(timeString).to(time);

    Assert.assertEquals("Should produce identical time",
        timeString, identity.toHumanString(timeLit.value()));
  }

  @Test
  public void testTimestampWithZoneHumanString() {
    Types.TimestampType timestamptz = Types.TimestampType.withZone();
    Transform<Long, Long> identity = Transforms.identity(timestamptz);

    Literal<Long> ts = Literal.of("2017-12-01T10:12:55.038194-08:00").to(timestamptz);

    // value will always be in UTC
    Assert.assertEquals("Should produce timestamp with time zone adjusted to UTC",
        "2017-12-01T18:12:55.038194Z", identity.toHumanString(ts.value()));
  }

  @Test
  public void testTimestampWithoutZoneHumanString() {
    Types.TimestampType timestamp = Types.TimestampType.withoutZone();
    Transform<Long, Long> identity = Transforms.identity(timestamp);

    String tsString = "2017-12-01T10:12:55.038194";
    Literal<Long> ts = Literal.of(tsString).to(timestamp);

    // value is not changed
    Assert.assertEquals("Should produce identical timestamp without time zone",
        tsString, identity.toHumanString(ts.value()));
  }

  @Test
  public void testLongToHumanString() {
    Types.LongType longType = Types.LongType.get();
    Transform<Long, Long> identity = Transforms.identity(longType);

    Assert.assertEquals("Should use Long toString",
        "-1234567890000", identity.toHumanString(-1234567890000L));
  }

  @Test
  public void testStringToHumanString() {
    Types.StringType string = Types.StringType.get();
    Transform<String, String> identity = Transforms.identity(string);

    String withSlash = "a/b/c=d";
    Assert.assertEquals("Should not modify Strings", withSlash, identity.toHumanString(withSlash));
  }

  @Test
  public void testBigDecimalToHumanString() {
    Types.DecimalType decimal = Types.DecimalType.of(9, 2);
    Transform<BigDecimal, BigDecimal> identity = Transforms.identity(decimal);

    String decimalString = "-1.50";
    BigDecimal bigDecimal = new BigDecimal(decimalString);
    Assert.assertEquals("Should not modify Strings", decimalString, identity.toHumanString(bigDecimal));
  }
}
