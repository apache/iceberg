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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.iceberg.spark.TestBaseWithCatalog;
import org.apache.iceberg.spark.functions.YearsFunction;
import org.apache.spark.sql.AnalysisException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

public class TestSparkYearsFunction extends TestBaseWithCatalog {

  @BeforeEach
  public void useCatalog() {
    sql("USE %s", catalogName);
  }

  @TestTemplate
  public void testDates() {
    assertThat(scalarSql("SELECT system.years(date('2017-12-01'))"))
        .as("Expected to produce 2017 - 1970 = 47")
        .isEqualTo(47);
    assertThat(scalarSql("SELECT system.years(date('1970-01-01'))"))
        .as("Expected to produce 1970 - 1970 = 0")
        .isEqualTo(0);
    assertThat(scalarSql("SELECT system.years(date('1969-12-31'))"))
        .as("Expected to produce 1969 - 1970 = -1")
        .isEqualTo(-1);
    assertThat(scalarSql("SELECT system.years(CAST(null AS DATE))")).isNull();
  }

  @TestTemplate
  public void testTimestamps() {
    assertThat(scalarSql("SELECT system.years(TIMESTAMP '2017-12-01 10:12:55.038194 UTC+00:00')"))
        .as("Expected to produce 2017 - 1970 = 47")
        .isEqualTo(47);
    assertThat(scalarSql("SELECT system.years(TIMESTAMP '1970-01-01 00:00:01.000001 UTC+00:00')"))
        .as("Expected to produce 1970 - 1970 = 0")
        .isEqualTo(0);
    assertThat(scalarSql("SELECT system.years(TIMESTAMP '1969-12-31 23:59:58.999999 UTC+00:00')"))
        .as("Expected to produce 1969 - 1970 = -1")
        .isEqualTo(-1);
    assertThat(scalarSql("SELECT system.years(CAST(null AS TIMESTAMP))")).isNull();
  }

  @TestTemplate
  public void testTimestampNtz() {
    assertThat(scalarSql("SELECT system.years(TIMESTAMP_NTZ '2017-12-01 10:12:55.038194 UTC')"))
        .as("Expected to produce 2017 - 1970 = 47")
        .isEqualTo(47);
    assertThat(scalarSql("SELECT system.years(TIMESTAMP_NTZ '1970-01-01 00:00:01.000001 UTC')"))
        .as("Expected to produce 1970 - 1970 = 0")
        .isEqualTo(0);
    assertThat(scalarSql("SELECT system.years(TIMESTAMP_NTZ '1969-12-31 23:59:58.999999 UTC')"))
        .as("Expected to produce 1969 - 1970 = -1")
        .isEqualTo(-1);
    assertThat(scalarSql("SELECT system.years(CAST(null AS TIMESTAMP_NTZ))")).isNull();
  }

  @TestTemplate
  public void testWrongNumberOfArguments() {
    assertThatThrownBy(() -> scalarSql("SELECT system.years()"))
        .isInstanceOf(AnalysisException.class)
        .hasMessageStartingWith(
            "Function 'years' cannot process input: (): Wrong number of inputs");

    assertThatThrownBy(
            () -> scalarSql("SELECT system.years(date('1969-12-31'), date('1969-12-31'))"))
        .isInstanceOf(AnalysisException.class)
        .hasMessageStartingWith(
            "Function 'years' cannot process input: (date, date): Wrong number of inputs");
  }

  @TestTemplate
  public void testInvalidInputTypes() {
    assertThatThrownBy(() -> scalarSql("SELECT system.years(1)"))
        .isInstanceOf(AnalysisException.class)
        .hasMessageStartingWith(
            "Function 'years' cannot process input: (int): Expected value to be date or timestamp");

    assertThatThrownBy(() -> scalarSql("SELECT system.years(1L)"))
        .isInstanceOf(AnalysisException.class)
        .hasMessageStartingWith(
            "Function 'years' cannot process input: (bigint): Expected value to be date or timestamp");
  }

  @TestTemplate
  public void testThatMagicFunctionsAreInvoked() {
    String dateValue = "date('2017-12-01')";
    String dateTransformClass = YearsFunction.DateToYearsFunction.class.getName();
    assertThat(scalarSql("EXPLAIN EXTENDED SELECT system.years(%s)", dateValue))
        .asString()
        .isNotNull()
        .contains("staticinvoke(class " + dateTransformClass);

    String timestampValue = "TIMESTAMP '2017-12-01 10:12:55.038194 UTC+00:00'";
    String timestampTransformClass = YearsFunction.TimestampToYearsFunction.class.getName();
    assertThat(scalarSql("EXPLAIN EXTENDED SELECT system.years(%s)", timestampValue))
        .asString()
        .isNotNull()
        .contains("staticinvoke(class " + timestampTransformClass);
  }
}
