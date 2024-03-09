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
package org.apache.iceberg.spark.data;

import java.time.ZoneId;
import java.util.TimeZone;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import org.apache.spark.sql.catalyst.util.TimestampFormatter;
import org.junit.Assert;
import org.junit.Test;

public class TestSparkDateTimes {
  @Test
  public void testSparkDate() {
    // checkSparkDate("1582-10-14"); // -141428
    checkSparkDate("1582-10-15"); // first day of the gregorian calendar
    checkSparkDate("1601-08-12");
    checkSparkDate("1801-07-04");
    checkSparkDate("1901-08-12");
    checkSparkDate("1969-12-31");
    checkSparkDate("1970-01-01");
    checkSparkDate("2017-12-25");
    checkSparkDate("2043-08-11");
    checkSparkDate("2111-05-03");
    checkSparkDate("2224-02-29");
    checkSparkDate("3224-10-05");
  }

  public void checkSparkDate(String dateString) {
    Literal<Integer> date = Literal.of(dateString).to(Types.DateType.get());
    String sparkDate = DateTimeUtils.toJavaDate(date.value()).toString();
    Assert.assertEquals("Should be the same date (" + date.value() + ")", dateString, sparkDate);
  }

  @Test
  public void testSparkTimestamp() {
    TimeZone currentTz = TimeZone.getDefault();
    try {
      TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
      checkSparkTimestamp("1582-10-15T15:51:08.440219+00:00", "1582-10-15 15:51:08.440219");
      checkSparkTimestamp("1970-01-01T00:00:00.000000+00:00", "1970-01-01 00:00:00");
      checkSparkTimestamp("2043-08-11T12:30:01.000001+00:00", "2043-08-11 12:30:01.000001");
    } finally {
      TimeZone.setDefault(currentTz);
    }
  }

  public void checkSparkTimestamp(String timestampString, String sparkRepr) {
    Literal<Long> ts = Literal.of(timestampString).to(Types.TimestampType.withZone());
    ZoneId zoneId = DateTimeUtils.getZoneId("UTC");
    TimestampFormatter formatter = TimestampFormatter.getFractionFormatter(zoneId);
    String sparkTimestamp = formatter.format(ts.value());
    Assert.assertEquals(
        "Should be the same timestamp (" + ts.value() + ")", sparkRepr, sparkTimestamp);
  }
}
