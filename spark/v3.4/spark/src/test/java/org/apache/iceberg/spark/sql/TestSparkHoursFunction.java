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
package org.apache.iceberg.spark.sql;

import org.apache.iceberg.spark.functions.HoursFunction;
import org.apache.spark.sql.AnalysisException;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.Assumptions;
import org.junit.Assert;
import org.junit.Test;

public class TestSparkHoursFunction extends SystemFunctionTestBase {

  public TestSparkHoursFunction(boolean systemFunctionPushDownEnabled) {
    super(systemFunctionPushDownEnabled);
  }

  @Test
  public void testTimestamps() {
    Assert.assertEquals(
        "Expected to produce 17501 * 24 + 10",
        420034,
        scalarSql("SELECT system.hours(TIMESTAMP '2017-12-01 10:12:55.038194 UTC+00:00')"));
    Assert.assertEquals(
        "Expected to produce 0 * 24 + 0 = 0",
        0,
        scalarSql("SELECT system.hours(TIMESTAMP '1970-01-01 00:00:01.000001 UTC+00:00')"));
    Assert.assertEquals(
        "Expected to produce -1",
        -1,
        scalarSql("SELECT system.hours(TIMESTAMP '1969-12-31 23:59:58.999999 UTC+00:00')"));
    Assert.assertNull(scalarSql("SELECT system.hours(CAST(null AS TIMESTAMP))"));
  }

  @Test
  public void testTimestampsNtz() {
    Assert.assertEquals(
        "Expected to produce 17501 * 24 + 10",
        420034,
        scalarSql("SELECT system.hours(TIMESTAMP_NTZ '2017-12-01 10:12:55.038194 UTC')"));
    Assert.assertEquals(
        "Expected to produce 0 * 24 + 0 = 0",
        0,
        scalarSql("SELECT system.hours(TIMESTAMP_NTZ '1970-01-01 00:00:01.000001 UTC')"));
    Assert.assertEquals(
        "Expected to produce -1",
        -1,
        scalarSql("SELECT system.hours(TIMESTAMP_NTZ '1969-12-31 23:59:58.999999 UTC')"));
    Assert.assertNull(scalarSql("SELECT system.hours(CAST(null AS TIMESTAMP_NTZ))"));
  }

  @Test
  public void testWrongNumberOfArguments() {
    Assertions.assertThatThrownBy(() -> scalarSql("SELECT system.hours()"))
        .isInstanceOf(AnalysisException.class)
        .hasMessageStartingWith(
            "Function 'hours' cannot process input: (): Wrong number of inputs");

    Assertions.assertThatThrownBy(
            () -> scalarSql("SELECT system.hours(date('1969-12-31'), date('1969-12-31'))"))
        .isInstanceOf(AnalysisException.class)
        .hasMessageStartingWith(
            "Function 'hours' cannot process input: (date, date): Wrong number of inputs");
  }

  @Test
  public void testInvalidInputTypes() {
    Assertions.assertThatThrownBy(() -> scalarSql("SELECT system.hours(1)"))
        .isInstanceOf(AnalysisException.class)
        .hasMessageStartingWith(
            "Function 'hours' cannot process input: (int): Expected value to be timestamp");

    Assertions.assertThatThrownBy(() -> scalarSql("SELECT system.hours(1L)"))
        .isInstanceOf(AnalysisException.class)
        .hasMessageStartingWith(
            "Function 'hours' cannot process input: (bigint): Expected value to be timestamp");
  }

  @Test
  public void testThatMagicFunctionsAreInvoked() {
    Assumptions.assumeThat(systemFunctionPushDownEnabled).isFalse();
    String timestampValue = "TIMESTAMP '2017-12-01 10:12:55.038194 UTC+00:00'";
    String timestampTransformClass = HoursFunction.TimestampToHoursFunction.class.getName();
    Assertions.assertThat(scalarSql("EXPLAIN EXTENDED SELECT system.hours(%s)", timestampValue))
        .asString()
        .isNotNull()
        .contains("staticinvoke(class " + timestampTransformClass);

    String timestampNtzValue = "TIMESTAMP_NTZ '2017-12-01 10:12:55.038194 UTC'";
    String timestampNtzTransformClass = HoursFunction.TimestampNtzToHoursFunction.class.getName();
    Assertions.assertThat(scalarSql("EXPLAIN EXTENDED SELECT system.hours(%s)", timestampNtzValue))
        .asString()
        .isNotNull()
        .contains("staticinvoke(class " + timestampNtzTransformClass);
  }

  @Test
  public void testAnalyzedToApplyFunctionExpression() {
    Assumptions.assumeThat(systemFunctionPushDownEnabled).isTrue();
    String timestampValue = "TIMESTAMP '2017-12-01 10:12:55.038194 UTC+00:00'";
    String timestampTransformCanonicalName =
        new HoursFunction.TimestampToHoursFunction().canonicalName();
    Assertions.assertThat(scalarSql("EXPLAIN EXTENDED SELECT system.hours(%s)", timestampValue))
        .asString()
        .isNotNull()
        .contains("applyfunctionexpression(Wrapper(" + timestampTransformCanonicalName);

    String timestampNtzValue = "TIMESTAMP_NTZ '2017-12-01 10:12:55.038194 UTC'";
    String timestampNtzTransformCanonicalName =
        new HoursFunction.TimestampNtzToHoursFunction().canonicalName();
    Assertions.assertThat(scalarSql("EXPLAIN EXTENDED SELECT system.hours(%s)", timestampNtzValue))
        .asString()
        .isNotNull()
        .contains("applyfunctionexpression(Wrapper(" + timestampNtzTransformCanonicalName);
  }
}
