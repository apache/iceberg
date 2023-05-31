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

import java.sql.Date;
import org.apache.iceberg.spark.SparkTestBaseWithCatalog;
import org.apache.spark.sql.AnalysisException;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestSparkDaysFunction extends SparkTestBaseWithCatalog {

  @Before
  public void useCatalog() {
    sql("USE %s", catalogName);
  }

  @Test
  public void testDates() {
    Assert.assertEquals(
        "Expected to produce 2017-12-01",
        Date.valueOf("2017-12-01"),
        scalarSql("SELECT system.days(date('2017-12-01'))"));
    Assert.assertEquals(
        "Expected to produce 1970-01-01",
        Date.valueOf("1970-01-01"),
        scalarSql("SELECT system.days(date('1970-01-01'))"));
    Assert.assertEquals(
        "Expected to produce 1969-12-31",
        Date.valueOf("1969-12-31"),
        scalarSql("SELECT system.days(date('1969-12-31'))"));
    Assert.assertNull(scalarSql("SELECT system.days(CAST(null AS DATE))"));
  }

  @Test
  public void testTimestamps() {
    Assert.assertEquals(
        "Expected to produce 2017-12-01",
        Date.valueOf("2017-12-01"),
        scalarSql("SELECT system.days(TIMESTAMP '2017-12-01 10:12:55.038194 UTC+00:00')"));
    Assert.assertEquals(
        "Expected to produce 1970-01-01",
        Date.valueOf("1970-01-01"),
        scalarSql("SELECT system.days(TIMESTAMP '1970-01-01 00:00:01.000001 UTC+00:00')"));
    Assert.assertEquals(
        "Expected to produce 1969-12-31",
        Date.valueOf("1969-12-31"),
        scalarSql("SELECT system.days(TIMESTAMP '1969-12-31 23:59:58.999999 UTC+00:00')"));
    Assert.assertNull(scalarSql("SELECT system.days(CAST(null AS TIMESTAMP))"));
  }

  @Test
  public void testTimestampNtz() {
    Assert.assertEquals(
        "Expected to produce 2017-12-01",
        Date.valueOf("2017-12-01"),
        scalarSql("SELECT system.days(TIMESTAMP_NTZ '2017-12-01 10:12:55.038194 UTC')"));
    Assert.assertEquals(
        "Expected to produce 1970-01-01",
        Date.valueOf("1970-01-01"),
        scalarSql("SELECT system.days(TIMESTAMP_NTZ '1970-01-01 00:00:01.000001 UTC')"));
    Assert.assertEquals(
        "Expected to produce 1969-12-31",
        Date.valueOf("1969-12-31"),
        scalarSql("SELECT system.days(TIMESTAMP_NTZ '1969-12-31 23:59:58.999999 UTC')"));
    Assert.assertNull(scalarSql("SELECT system.days(CAST(null AS TIMESTAMP_NTZ))"));
  }

  @Test
  public void testWrongNumberOfArguments() {
    Assertions.assertThatThrownBy(() -> scalarSql("SELECT system.days()"))
        .isInstanceOf(AnalysisException.class)
        .hasMessageStartingWith("Function 'days' cannot process input: (): Wrong number of inputs");

    Assertions.assertThatThrownBy(
            () -> scalarSql("SELECT system.days(date('1969-12-31'), date('1969-12-31'))"))
        .isInstanceOf(AnalysisException.class)
        .hasMessageStartingWith(
            "Function 'days' cannot process input: (date, date): Wrong number of inputs");
  }

  @Test
  public void testInvalidInputTypes() {
    Assertions.assertThatThrownBy(() -> scalarSql("SELECT system.days(1)"))
        .isInstanceOf(AnalysisException.class)
        .hasMessageStartingWith(
            "Function 'days' cannot process input: (int): Expected value to be date or timestamp");

    Assertions.assertThatThrownBy(() -> scalarSql("SELECT system.days(1L)"))
        .isInstanceOf(AnalysisException.class)
        .hasMessageStartingWith(
            "Function 'days' cannot process input: (bigint): Expected value to be date or timestamp");
  }
}
