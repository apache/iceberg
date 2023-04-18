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

import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.spark.SparkTestBaseWithCatalog;
import org.apache.iceberg.spark.functions.MonthsFunction;
import org.apache.spark.sql.AnalysisException;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestSparkMonthsFunction extends SparkTestBaseWithCatalog {

  @Before
  public void useCatalog() {
    sql("USE %s", catalogName);
  }

  @Test
  public void testDates() {
    Assert.assertEquals(
        "Expected to produce 47 * 12 + 11 = 575",
        575,
        scalarSql("SELECT system.months(date('2017-12-01'))"));
    Assert.assertEquals(
        "Expected to produce 0 * 12 + 0 = 0",
        0,
        scalarSql("SELECT system.months(date('1970-01-01'))"));
    Assert.assertEquals(
        "Expected to produce -1", -1, scalarSql("SELECT system.months(date('1969-12-31'))"));
    Assert.assertNull(scalarSql("SELECT system.months(CAST(null AS DATE))"));
  }

  @Test
  public void testTimestamps() {
    Assert.assertEquals(
        "Expected to produce 47 * 12 + 11 = 575",
        575,
        scalarSql("SELECT system.months(TIMESTAMP '2017-12-01 10:12:55.038194 UTC+00:00')"));
    Assert.assertEquals(
        "Expected to produce 0 * 12 + 0 = 0",
        0,
        scalarSql("SELECT system.months(TIMESTAMP '1970-01-01 00:00:01.000001 UTC+00:00')"));
    Assert.assertEquals(
        "Expected to produce -1",
        -1,
        scalarSql("SELECT system.months(TIMESTAMP '1969-12-31 23:59:58.999999 UTC+00:00')"));
    Assert.assertNull(scalarSql("SELECT system.months(CAST(null AS TIMESTAMP))"));
  }

  @Test
  public void testWrongNumberOfArguments() {
    AssertHelpers.assertThrows(
        "Function resolution should not work with zero arguments",
        AnalysisException.class,
        "Function 'months' cannot process input: (): Wrong number of inputs",
        () -> scalarSql("SELECT system.months()"));

    AssertHelpers.assertThrows(
        "Function resolution should not work with more than one argument",
        AnalysisException.class,
        "Function 'months' cannot process input: (date, date): Wrong number of inputs",
        () -> scalarSql("SELECT system.months(date('1969-12-31'), date('1969-12-31'))"));
  }

  @Test
  public void testInvalidInputTypes() {
    AssertHelpers.assertThrows(
        "Int type should not be coercible to date/timestamp",
        AnalysisException.class,
        "Function 'months' cannot process input: (int): Expected value to be date or timestamp",
        () -> scalarSql("SELECT system.months(1)"));

    AssertHelpers.assertThrows(
        "Long type should not be coercible to date/timestamp",
        AnalysisException.class,
        "Function 'months' cannot process input: (bigint): Expected value to be date or timestamp",
        () -> scalarSql("SELECT system.months(1L)"));
  }

  @Test
  public void testThatMagicFunctionsAreInvoked() {
    String dateValue = "date('2017-12-01')";
    String dateTransformClass = MonthsFunction.DateToMonthsFunction.class.getName();
    Assertions.assertThat(scalarSql("EXPLAIN EXTENDED SELECT system.months(%s)", dateValue))
        .asString()
        .isNotNull()
        .contains("staticinvoke(class " + dateTransformClass);

    String timestampValue = "TIMESTAMP '2017-12-01 10:12:55.038194 UTC+00:00'";
    String timestampTransformClass = MonthsFunction.TimestampToMonthsFunction.class.getName();
    Assertions.assertThat(scalarSql("EXPLAIN EXTENDED SELECT system.months(%s)", timestampValue))
        .asString()
        .isNotNull()
        .contains("staticinvoke(class " + timestampTransformClass);
  }
}
