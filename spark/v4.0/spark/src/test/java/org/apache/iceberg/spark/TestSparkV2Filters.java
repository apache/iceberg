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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionUtil;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.UnboundTerm;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.functions.BucketFunction;
import org.apache.iceberg.spark.functions.DaysFunction;
import org.apache.iceberg.spark.functions.HoursFunction;
import org.apache.iceberg.spark.functions.IcebergVersionFunction;
import org.apache.iceberg.spark.functions.MonthsFunction;
import org.apache.iceberg.spark.functions.TruncateFunction;
import org.apache.iceberg.spark.functions.YearsFunction;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.spark.sql.connector.catalog.functions.ScalarFunction;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.LiteralValue;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.UserDefinedScalarFunc;
import org.apache.spark.sql.connector.expressions.filter.And;
import org.apache.spark.sql.connector.expressions.filter.Not;
import org.apache.spark.sql.connector.expressions.filter.Or;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Test;

public class TestSparkV2Filters {

  private static final Types.StructType STRUCT =
      Types.StructType.of(
          Types.NestedField.optional(1, "dateCol", Types.DateType.get()),
          Types.NestedField.optional(2, "tsCol", Types.TimestampType.withZone()),
          Types.NestedField.optional(3, "tsNtzCol", Types.TimestampType.withoutZone()),
          Types.NestedField.optional(4, "intCol", Types.IntegerType.get()),
          Types.NestedField.optional(5, "strCol", Types.StringType.get()));

  @SuppressWarnings("checkstyle:MethodLength")
  @Test
  public void testV2Filters() {
    Map<String, String> attrMap = Maps.newHashMap();
    attrMap.put("id", "id");
    attrMap.put("`i.d`", "i.d");
    attrMap.put("`i``d`", "i`d");
    attrMap.put("`d`.b.`dd```", "d.b.dd`");
    attrMap.put("a.`aa```.c", "a.aa`.c");

    attrMap.forEach(
        (quoted, unquoted) -> {
          NamedReference namedReference = FieldReference.apply(quoted);
          org.apache.spark.sql.connector.expressions.Expression[] attrOnly =
              new org.apache.spark.sql.connector.expressions.Expression[] {namedReference};

          LiteralValue value = new LiteralValue(1, DataTypes.IntegerType);
          org.apache.spark.sql.connector.expressions.Expression[] attrAndValue =
              new org.apache.spark.sql.connector.expressions.Expression[] {namedReference, value};
          org.apache.spark.sql.connector.expressions.Expression[] valueAndAttr =
              new org.apache.spark.sql.connector.expressions.Expression[] {value, namedReference};

          Predicate isNull = new Predicate("IS_NULL", attrOnly);
          Expression expectedIsNull = Expressions.isNull(unquoted);
          Expression actualIsNull = SparkV2Filters.convert(isNull);
          assertThat(actualIsNull.toString())
              .as("IsNull must match")
              .isEqualTo(expectedIsNull.toString());

          Predicate isNotNull = new Predicate("IS_NOT_NULL", attrOnly);
          Expression expectedIsNotNull = Expressions.notNull(unquoted);
          Expression actualIsNotNull = SparkV2Filters.convert(isNotNull);
          assertThat(actualIsNotNull.toString())
              .as("IsNotNull must match")
              .isEqualTo(expectedIsNotNull.toString());

          Predicate lt1 = new Predicate("<", attrAndValue);
          Expression expectedLt1 = Expressions.lessThan(unquoted, 1);
          Expression actualLt1 = SparkV2Filters.convert(lt1);
          assertThat(actualLt1.toString())
              .as("LessThan must match")
              .isEqualTo(expectedLt1.toString());

          Predicate lt2 = new Predicate("<", valueAndAttr);
          Expression expectedLt2 = Expressions.greaterThan(unquoted, 1);
          Expression actualLt2 = SparkV2Filters.convert(lt2);
          assertThat(actualLt2.toString())
              .as("LessThan must match")
              .isEqualTo(expectedLt2.toString());

          Predicate ltEq1 = new Predicate("<=", attrAndValue);
          Expression expectedLtEq1 = Expressions.lessThanOrEqual(unquoted, 1);
          Expression actualLtEq1 = SparkV2Filters.convert(ltEq1);
          assertThat(actualLtEq1.toString())
              .as("LessThanOrEqual must match")
              .isEqualTo(expectedLtEq1.toString());

          Predicate ltEq2 = new Predicate("<=", valueAndAttr);
          Expression expectedLtEq2 = Expressions.greaterThanOrEqual(unquoted, 1);
          Expression actualLtEq2 = SparkV2Filters.convert(ltEq2);
          assertThat(actualLtEq2.toString())
              .as("LessThanOrEqual must match")
              .isEqualTo(expectedLtEq2.toString());

          Predicate gt1 = new Predicate(">", attrAndValue);
          Expression expectedGt1 = Expressions.greaterThan(unquoted, 1);
          Expression actualGt1 = SparkV2Filters.convert(gt1);
          assertThat(actualGt1.toString())
              .as("GreaterThan must match")
              .isEqualTo(expectedGt1.toString());

          Predicate gt2 = new Predicate(">", valueAndAttr);
          Expression expectedGt2 = Expressions.lessThan(unquoted, 1);
          Expression actualGt2 = SparkV2Filters.convert(gt2);
          assertThat(actualGt2.toString())
              .as("GreaterThan must match")
              .isEqualTo(expectedGt2.toString());

          Predicate gtEq1 = new Predicate(">=", attrAndValue);
          Expression expectedGtEq1 = Expressions.greaterThanOrEqual(unquoted, 1);
          Expression actualGtEq1 = SparkV2Filters.convert(gtEq1);
          assertThat(actualGtEq1.toString())
              .as("GreaterThanOrEqual must match")
              .isEqualTo(expectedGtEq1.toString());

          Predicate gtEq2 = new Predicate(">=", valueAndAttr);
          Expression expectedGtEq2 = Expressions.lessThanOrEqual(unquoted, 1);
          Expression actualGtEq2 = SparkV2Filters.convert(gtEq2);
          assertThat(actualGtEq2.toString())
              .as("GreaterThanOrEqual must match")
              .isEqualTo(expectedGtEq2.toString());

          Predicate eq1 = new Predicate("=", attrAndValue);
          Expression expectedEq1 = Expressions.equal(unquoted, 1);
          Expression actualEq1 = SparkV2Filters.convert(eq1);
          assertThat(actualEq1.toString())
              .as("EqualTo must match")
              .isEqualTo(expectedEq1.toString());

          Predicate eq2 = new Predicate("=", valueAndAttr);
          Expression expectedEq2 = Expressions.equal(unquoted, 1);
          Expression actualEq2 = SparkV2Filters.convert(eq2);
          assertThat(actualEq2.toString())
              .as("EqualTo must match")
              .isEqualTo(expectedEq2.toString());

          Predicate notEq1 = new Predicate("<>", attrAndValue);
          Expression expectedNotEq1 = Expressions.notEqual(unquoted, 1);
          Expression actualNotEq1 = SparkV2Filters.convert(notEq1);
          assertThat(actualNotEq1.toString())
              .as("NotEqualTo must match")
              .isEqualTo(expectedNotEq1.toString());

          Predicate notEq2 = new Predicate("<>", valueAndAttr);
          Expression expectedNotEq2 = Expressions.notEqual(unquoted, 1);
          Expression actualNotEq2 = SparkV2Filters.convert(notEq2);
          assertThat(actualNotEq2.toString())
              .as("NotEqualTo must match")
              .isEqualTo(expectedNotEq2.toString());

          Predicate eqNullSafe1 = new Predicate("<=>", attrAndValue);
          Expression expectedEqNullSafe1 = Expressions.equal(unquoted, 1);
          Expression actualEqNullSafe1 = SparkV2Filters.convert(eqNullSafe1);
          assertThat(actualEqNullSafe1.toString())
              .as("EqualNullSafe must match")
              .isEqualTo(expectedEqNullSafe1.toString());

          Predicate eqNullSafe2 = new Predicate("<=>", valueAndAttr);
          Expression expectedEqNullSafe2 = Expressions.equal(unquoted, 1);
          Expression actualEqNullSafe2 = SparkV2Filters.convert(eqNullSafe2);
          assertThat(actualEqNullSafe2.toString())
              .as("EqualNullSafe must match")
              .isEqualTo(expectedEqNullSafe2.toString());

          LiteralValue str =
              new LiteralValue(UTF8String.fromString("iceberg"), DataTypes.StringType);
          org.apache.spark.sql.connector.expressions.Expression[] attrAndStr =
              new org.apache.spark.sql.connector.expressions.Expression[] {namedReference, str};
          Predicate startsWith = new Predicate("STARTS_WITH", attrAndStr);
          Expression expectedStartsWith = Expressions.startsWith(unquoted, "iceberg");
          Expression actualStartsWith = SparkV2Filters.convert(startsWith);
          assertThat(actualStartsWith.toString())
              .as("StartsWith must match")
              .isEqualTo(expectedStartsWith.toString());

          Predicate in = new Predicate("IN", attrAndValue);
          Expression expectedIn = Expressions.in(unquoted, 1);
          Expression actualIn = SparkV2Filters.convert(in);
          assertThat(actualIn.toString()).as("In must match").isEqualTo(expectedIn.toString());

          Predicate and = new And(lt1, eq1);
          Expression expectedAnd = Expressions.and(expectedLt1, expectedEq1);
          Expression actualAnd = SparkV2Filters.convert(and);
          assertThat(actualAnd.toString()).as("And must match").isEqualTo(expectedAnd.toString());

          org.apache.spark.sql.connector.expressions.Expression[] attrAndAttr =
              new org.apache.spark.sql.connector.expressions.Expression[] {
                namedReference, namedReference
              };
          Predicate invalid = new Predicate("<", attrAndAttr);
          Predicate andWithInvalidLeft = new And(invalid, eq1);
          Expression convertedAnd = SparkV2Filters.convert(andWithInvalidLeft);
          assertThat(convertedAnd).as("And must match").isNull();

          Predicate or = new Or(lt1, eq1);
          Expression expectedOr = Expressions.or(expectedLt1, expectedEq1);
          Expression actualOr = SparkV2Filters.convert(or);
          assertThat(actualOr.toString()).as("Or must match").isEqualTo(expectedOr.toString());

          Predicate orWithInvalidLeft = new Or(invalid, eq1);
          Expression convertedOr = SparkV2Filters.convert(orWithInvalidLeft);
          assertThat(convertedOr).as("Or must match").isNull();

          Predicate not = new Not(lt1);
          Expression expectedNot = Expressions.not(expectedLt1);
          Expression actualNot = SparkV2Filters.convert(not);
          assertThat(actualNot.toString()).as("Not must match").isEqualTo(expectedNot.toString());
        });
  }

  @Test
  public void testEqualToNull() {
    String col = "col";
    NamedReference namedReference = FieldReference.apply(col);
    LiteralValue value = new LiteralValue(null, DataTypes.IntegerType);

    org.apache.spark.sql.connector.expressions.Expression[] attrAndValue =
        new org.apache.spark.sql.connector.expressions.Expression[] {namedReference, value};
    org.apache.spark.sql.connector.expressions.Expression[] valueAndAttr =
        new org.apache.spark.sql.connector.expressions.Expression[] {value, namedReference};

    Predicate eq1 = new Predicate("=", attrAndValue);
    assertThatThrownBy(() -> SparkV2Filters.convert(eq1))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("Expression is always false");

    Predicate eq2 = new Predicate("=", valueAndAttr);
    assertThatThrownBy(() -> SparkV2Filters.convert(eq2))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("Expression is always false");

    Predicate eqNullSafe1 = new Predicate("<=>", attrAndValue);
    Expression expectedEqNullSafe = Expressions.isNull(col);
    Expression actualEqNullSafe1 = SparkV2Filters.convert(eqNullSafe1);
    assertThat(actualEqNullSafe1.toString()).isEqualTo(expectedEqNullSafe.toString());

    Predicate eqNullSafe2 = new Predicate("<=>", valueAndAttr);
    Expression actualEqNullSafe2 = SparkV2Filters.convert(eqNullSafe2);
    assertThat(actualEqNullSafe2.toString()).isEqualTo(expectedEqNullSafe.toString());
  }

  @Test
  public void testEqualToNaN() {
    String col = "col";
    NamedReference namedReference = FieldReference.apply(col);
    LiteralValue value = new LiteralValue(Float.NaN, DataTypes.FloatType);

    org.apache.spark.sql.connector.expressions.Expression[] attrAndValue =
        new org.apache.spark.sql.connector.expressions.Expression[] {namedReference, value};
    org.apache.spark.sql.connector.expressions.Expression[] valueAndAttr =
        new org.apache.spark.sql.connector.expressions.Expression[] {value, namedReference};

    Predicate eqNaN1 = new Predicate("=", attrAndValue);
    Expression expectedEqNaN = Expressions.isNaN(col);
    Expression actualEqNaN1 = SparkV2Filters.convert(eqNaN1);
    assertThat(actualEqNaN1.toString()).isEqualTo(expectedEqNaN.toString());

    Predicate eqNaN2 = new Predicate("=", valueAndAttr);
    Expression actualEqNaN2 = SparkV2Filters.convert(eqNaN2);
    assertThat(actualEqNaN2.toString()).isEqualTo(expectedEqNaN.toString());
  }

  @Test
  public void testNotEqualToNull() {
    String col = "col";
    NamedReference namedReference = FieldReference.apply(col);
    LiteralValue value = new LiteralValue(null, DataTypes.IntegerType);

    org.apache.spark.sql.connector.expressions.Expression[] attrAndValue =
        new org.apache.spark.sql.connector.expressions.Expression[] {namedReference, value};
    org.apache.spark.sql.connector.expressions.Expression[] valueAndAttr =
        new org.apache.spark.sql.connector.expressions.Expression[] {value, namedReference};

    Predicate notEq1 = new Predicate("<>", attrAndValue);
    assertThatThrownBy(() -> SparkV2Filters.convert(notEq1))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("Expression is always false");

    Predicate notEq2 = new Predicate("<>", valueAndAttr);
    assertThatThrownBy(() -> SparkV2Filters.convert(notEq2))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("Expression is always false");
  }

  @Test
  public void testNotEqualToNaN() {
    String col = "col";
    NamedReference namedReference = FieldReference.apply(col);
    LiteralValue value = new LiteralValue(Float.NaN, DataTypes.FloatType);

    org.apache.spark.sql.connector.expressions.Expression[] attrAndValue =
        new org.apache.spark.sql.connector.expressions.Expression[] {namedReference, value};
    org.apache.spark.sql.connector.expressions.Expression[] valueAndAttr =
        new org.apache.spark.sql.connector.expressions.Expression[] {value, namedReference};

    Predicate notEqNaN1 = new Predicate("<>", attrAndValue);
    Expression expectedNotEqNaN = Expressions.notNaN(col);
    Expression actualNotEqNaN1 = SparkV2Filters.convert(notEqNaN1);
    assertThat(actualNotEqNaN1.toString()).isEqualTo(expectedNotEqNaN.toString());

    Predicate notEqNaN2 = new Predicate("<>", valueAndAttr);
    Expression actualNotEqNaN2 = SparkV2Filters.convert(notEqNaN2);
    assertThat(actualNotEqNaN2.toString()).isEqualTo(expectedNotEqNaN.toString());
  }

  @Test
  public void testInValuesContainNull() {
    String col = "strCol";
    NamedReference namedReference = FieldReference.apply(col);
    LiteralValue nullValue = new LiteralValue(null, DataTypes.StringType);
    LiteralValue value1 = new LiteralValue("value1", DataTypes.StringType);
    LiteralValue value2 = new LiteralValue("value2", DataTypes.StringType);

    // Values only contains null
    Predicate inNull = new Predicate("IN", expressions(namedReference, nullValue));
    Expression expectedInNull = Expressions.in(col);
    Expression actualInNull = SparkV2Filters.convert(inNull);
    assertEquals(expectedInNull, actualInNull);

    Predicate in = new Predicate("IN", expressions(namedReference, nullValue, value1, value2));
    Expression expectedIn = Expressions.in(col, "value1", "value2");
    Expression actualIn = SparkV2Filters.convert(in);
    assertEquals(expectedIn, actualIn);
  }

  @Test
  public void testNotInNull() {
    String col = "strCol";
    NamedReference namedReference = FieldReference.apply(col);
    LiteralValue nullValue = new LiteralValue(null, DataTypes.StringType);
    LiteralValue value1 = new LiteralValue("value1", DataTypes.StringType);
    LiteralValue value2 = new LiteralValue("value2", DataTypes.StringType);

    // Values only contains null
    Predicate notInNull = new Not(new Predicate("IN", expressions(namedReference, nullValue)));
    Expression expectedNotInNull =
        Expressions.and(Expressions.notNull(col), Expressions.notIn(col));
    Expression actualNotInNull = SparkV2Filters.convert(notInNull);
    assertEquals(expectedNotInNull, actualNotInNull);

    Predicate notIn =
        new Not(new Predicate("IN", expressions(namedReference, nullValue, value1, value2)));
    Expression expectedNotIn =
        Expressions.and(Expressions.notNull(col), Expressions.notIn(col, "value1", "value2"));
    Expression actualNotIn = SparkV2Filters.convert(notIn);
    assertEquals(expectedNotIn, actualNotIn);
  }

  @Test
  public void testTimestampFilterConversion() {
    Instant instant = Instant.parse("2018-10-18T00:00:57.907Z");
    long epochMicros = ChronoUnit.MICROS.between(Instant.EPOCH, instant);

    NamedReference namedReference = FieldReference.apply("x");
    LiteralValue ts = new LiteralValue(epochMicros, DataTypes.TimestampType);
    org.apache.spark.sql.connector.expressions.Expression[] attrAndValue =
        new org.apache.spark.sql.connector.expressions.Expression[] {namedReference, ts};

    Predicate predicate = new Predicate(">", attrAndValue);
    Expression tsExpression = SparkV2Filters.convert(predicate);
    Expression rawExpression = Expressions.greaterThan("x", epochMicros);

    assertThat(tsExpression.toString())
        .as("Generated Timestamp expression should be correct")
        .isEqualTo(rawExpression.toString());
  }

  @Test
  public void testDateFilterConversion() {
    LocalDate localDate = LocalDate.parse("2018-10-18");
    long epochDay = localDate.toEpochDay();

    NamedReference namedReference = FieldReference.apply("x");
    LiteralValue ts = new LiteralValue(epochDay, DataTypes.DateType);
    org.apache.spark.sql.connector.expressions.Expression[] attrAndValue =
        new org.apache.spark.sql.connector.expressions.Expression[] {namedReference, ts};

    Predicate predicate = new Predicate(">", attrAndValue);
    Expression dateExpression = SparkV2Filters.convert(predicate);
    Expression rawExpression = Expressions.greaterThan("x", epochDay);

    assertThat(dateExpression.toString())
        .as("Generated date expression should be correct")
        .isEqualTo(rawExpression.toString());
  }

  @Test
  public void testNestedInInsideNot() {
    NamedReference namedReference1 = FieldReference.apply("col1");
    LiteralValue v1 = new LiteralValue(1, DataTypes.IntegerType);
    LiteralValue v2 = new LiteralValue(2, DataTypes.IntegerType);
    org.apache.spark.sql.connector.expressions.Expression[] attrAndValue1 =
        new org.apache.spark.sql.connector.expressions.Expression[] {namedReference1, v1};
    Predicate equal = new Predicate("=", attrAndValue1);

    NamedReference namedReference2 = FieldReference.apply("col2");
    org.apache.spark.sql.connector.expressions.Expression[] attrAndValue2 =
        new org.apache.spark.sql.connector.expressions.Expression[] {namedReference2, v1, v2};
    Predicate in = new Predicate("IN", attrAndValue2);

    Not filter = new Not(new And(equal, in));
    Expression converted = SparkV2Filters.convert(filter);
    assertThat(converted).as("Expression should not be converted").isNull();
  }

  @Test
  public void testNotIn() {
    NamedReference namedReference = FieldReference.apply("col");
    LiteralValue v1 = new LiteralValue(1, DataTypes.IntegerType);
    LiteralValue v2 = new LiteralValue(2, DataTypes.IntegerType);
    org.apache.spark.sql.connector.expressions.Expression[] attrAndValue =
        new org.apache.spark.sql.connector.expressions.Expression[] {namedReference, v1, v2};

    Predicate in = new Predicate("IN", attrAndValue);
    Not not = new Not(in);

    Expression actual = SparkV2Filters.convert(not);
    Expression expected =
        Expressions.and(Expressions.notNull("col"), Expressions.notIn("col", 1, 2));
    assertThat(actual.toString()).as("Expressions should match").isEqualTo(expected.toString());
  }

  @Test
  public void testDateToYears() {
    ScalarFunction<Integer> dateToYearsFunc = new YearsFunction.DateToYearsFunction();
    UserDefinedScalarFunc udf =
        new UserDefinedScalarFunc(
            dateToYearsFunc.name(),
            dateToYearsFunc.canonicalName(),
            expressions(FieldReference.apply("dateCol")));
    testUDF(udf, Expressions.year("dateCol"), dateToYears("2023-06-25"), DataTypes.IntegerType);
  }

  @Test
  public void testTsToYears() {
    ScalarFunction<Integer> tsToYearsFunc = new YearsFunction.TimestampToYearsFunction();
    UserDefinedScalarFunc udf =
        new UserDefinedScalarFunc(
            tsToYearsFunc.name(),
            tsToYearsFunc.canonicalName(),
            expressions(FieldReference.apply("tsCol")));
    testUDF(
        udf,
        Expressions.year("tsCol"),
        timestampToYears("2023-12-03T10:15:30+01:00"),
        DataTypes.IntegerType);
  }

  @Test
  public void testTsNtzToYears() {
    ScalarFunction<Integer> tsNtzToYearsFunc = new YearsFunction.TimestampNtzToYearsFunction();
    UserDefinedScalarFunc udf =
        new UserDefinedScalarFunc(
            tsNtzToYearsFunc.name(),
            tsNtzToYearsFunc.canonicalName(),
            expressions(FieldReference.apply("tsNtzCol")));
    testUDF(
        udf,
        Expressions.year("tsNtzCol"),
        timestampNtzToYears("2023-06-25T13:15:30"),
        DataTypes.IntegerType);
  }

  @Test
  public void testDateToMonths() {
    ScalarFunction<Integer> dateToMonthsFunc = new MonthsFunction.DateToMonthsFunction();
    UserDefinedScalarFunc udf =
        new UserDefinedScalarFunc(
            dateToMonthsFunc.name(),
            dateToMonthsFunc.canonicalName(),
            expressions(FieldReference.apply("dateCol")));
    testUDF(udf, Expressions.month("dateCol"), dateToMonths("2023-06-25"), DataTypes.IntegerType);
  }

  @Test
  public void testTsToMonths() {
    ScalarFunction<Integer> tsToMonthsFunc = new MonthsFunction.TimestampToMonthsFunction();
    UserDefinedScalarFunc udf =
        new UserDefinedScalarFunc(
            tsToMonthsFunc.name(),
            tsToMonthsFunc.canonicalName(),
            expressions(FieldReference.apply("tsCol")));
    testUDF(
        udf,
        Expressions.month("tsCol"),
        timestampToMonths("2023-12-03T10:15:30+01:00"),
        DataTypes.IntegerType);
  }

  @Test
  public void testTsNtzToMonths() {
    ScalarFunction<Integer> tsNtzToMonthsFunc = new MonthsFunction.TimestampNtzToMonthsFunction();
    UserDefinedScalarFunc udf =
        new UserDefinedScalarFunc(
            tsNtzToMonthsFunc.name(),
            tsNtzToMonthsFunc.canonicalName(),
            expressions(FieldReference.apply("tsNtzCol")));
    testUDF(
        udf,
        Expressions.month("tsNtzCol"),
        timestampNtzToMonths("2023-12-03T10:15:30"),
        DataTypes.IntegerType);
  }

  @Test
  public void testDateToDays() {
    ScalarFunction<Integer> dateToDayFunc = new DaysFunction.DateToDaysFunction();
    UserDefinedScalarFunc udf =
        new UserDefinedScalarFunc(
            dateToDayFunc.name(),
            dateToDayFunc.canonicalName(),
            expressions(FieldReference.apply("dateCol")));
    testUDF(udf, Expressions.day("dateCol"), dateToDays("2023-06-25"), DataTypes.IntegerType);
  }

  @Test
  public void testTsToDays() {
    ScalarFunction<Integer> tsToDaysFunc = new DaysFunction.TimestampToDaysFunction();
    UserDefinedScalarFunc udf =
        new UserDefinedScalarFunc(
            tsToDaysFunc.name(),
            tsToDaysFunc.canonicalName(),
            expressions(FieldReference.apply("tsCol")));
    testUDF(
        udf,
        Expressions.day("tsCol"),
        timestampToDays("2023-12-03T10:15:30+01:00"),
        DataTypes.IntegerType);
  }

  @Test
  public void testTsNtzToDays() {
    ScalarFunction<Integer> tsNtzToDaysFunc = new DaysFunction.TimestampNtzToDaysFunction();
    UserDefinedScalarFunc udf =
        new UserDefinedScalarFunc(
            tsNtzToDaysFunc.name(),
            tsNtzToDaysFunc.canonicalName(),
            expressions(FieldReference.apply("tsNtzCol")));
    testUDF(
        udf,
        Expressions.day("tsNtzCol"),
        timestampNtzToDays("2023-12-03T10:15:30"),
        DataTypes.IntegerType);
  }

  @Test
  public void testTsToHours() {
    ScalarFunction<Integer> tsToHourFunc = new HoursFunction.TimestampToHoursFunction();
    UserDefinedScalarFunc udf =
        new UserDefinedScalarFunc(
            tsToHourFunc.name(),
            tsToHourFunc.canonicalName(),
            expressions(FieldReference.apply("tsCol")));
    testUDF(
        udf,
        Expressions.hour("tsCol"),
        timestampToHours("2023-12-03T10:15:30+01:00"),
        DataTypes.IntegerType);
  }

  @Test
  public void testTsNtzToHours() {
    ScalarFunction<Integer> tsNtzToHourFunc = new HoursFunction.TimestampNtzToHoursFunction();
    UserDefinedScalarFunc udf =
        new UserDefinedScalarFunc(
            tsNtzToHourFunc.name(),
            tsNtzToHourFunc.canonicalName(),
            expressions(FieldReference.apply("tsNtzCol")));
    testUDF(
        udf,
        Expressions.hour("tsNtzCol"),
        timestampNtzToHours("2023-12-03T10:15:30"),
        DataTypes.IntegerType);
  }

  @Test
  public void testBucket() {
    ScalarFunction<Integer> bucketInt = new BucketFunction.BucketInt(DataTypes.IntegerType);
    UserDefinedScalarFunc udf =
        new UserDefinedScalarFunc(
            bucketInt.name(),
            bucketInt.canonicalName(),
            expressions(
                LiteralValue.apply(4, DataTypes.IntegerType), FieldReference.apply("intCol")));
    testUDF(udf, Expressions.bucket("intCol", 4), 2, DataTypes.IntegerType);
  }

  @Test
  public void testTruncate() {
    ScalarFunction<UTF8String> truncate = new TruncateFunction.TruncateString();
    UserDefinedScalarFunc udf =
        new UserDefinedScalarFunc(
            truncate.name(),
            truncate.canonicalName(),
            expressions(
                LiteralValue.apply(6, DataTypes.IntegerType), FieldReference.apply("strCol")));
    testUDF(udf, Expressions.truncate("strCol", 6), "prefix", DataTypes.StringType);
  }

  @Test
  public void testUnsupportedUDFConvert() {
    ScalarFunction<UTF8String> icebergVersionFunc =
        (ScalarFunction<UTF8String>) new IcebergVersionFunction().bind(new StructType());
    UserDefinedScalarFunc udf =
        new UserDefinedScalarFunc(
            icebergVersionFunc.name(),
            icebergVersionFunc.canonicalName(),
            new org.apache.spark.sql.connector.expressions.Expression[] {});
    LiteralValue literalValue = new LiteralValue("1.3.0", DataTypes.StringType);
    Predicate predicate = new Predicate("=", expressions(udf, literalValue));

    Expression icebergExpr = SparkV2Filters.convert(predicate);
    assertThat(icebergExpr).isNull();
  }

  private <T> void testUDF(
      org.apache.spark.sql.connector.expressions.Expression udf,
      UnboundTerm<T> expectedTerm,
      T value,
      DataType dataType) {
    org.apache.spark.sql.connector.expressions.Expression[] attrOnly = expressions(udf);

    LiteralValue literalValue = new LiteralValue(value, dataType);
    org.apache.spark.sql.connector.expressions.Expression[] attrAndValue =
        expressions(udf, literalValue);
    org.apache.spark.sql.connector.expressions.Expression[] valueAndAttr =
        expressions(literalValue, udf);

    Predicate isNull = new Predicate("IS_NULL", attrOnly);
    Expression expectedIsNull = Expressions.isNull(expectedTerm);
    Expression actualIsNull = SparkV2Filters.convert(isNull);
    assertEquals(expectedIsNull, actualIsNull);

    Predicate isNotNull = new Predicate("IS_NOT_NULL", attrOnly);
    Expression expectedIsNotNull = Expressions.notNull(expectedTerm);
    Expression actualIsNotNull = SparkV2Filters.convert(isNotNull);
    assertEquals(expectedIsNotNull, actualIsNotNull);

    Predicate lt1 = new Predicate("<", attrAndValue);
    Expression expectedLt1 = Expressions.lessThan(expectedTerm, value);
    Expression actualLt1 = SparkV2Filters.convert(lt1);
    assertEquals(expectedLt1, actualLt1);

    Predicate lt2 = new Predicate("<", valueAndAttr);
    Expression expectedLt2 = Expressions.greaterThan(expectedTerm, value);
    Expression actualLt2 = SparkV2Filters.convert(lt2);
    assertEquals(expectedLt2, actualLt2);

    Predicate ltEq1 = new Predicate("<=", attrAndValue);
    Expression expectedLtEq1 = Expressions.lessThanOrEqual(expectedTerm, value);
    Expression actualLtEq1 = SparkV2Filters.convert(ltEq1);
    assertEquals(expectedLtEq1, actualLtEq1);

    Predicate ltEq2 = new Predicate("<=", valueAndAttr);
    Expression expectedLtEq2 = Expressions.greaterThanOrEqual(expectedTerm, value);
    Expression actualLtEq2 = SparkV2Filters.convert(ltEq2);
    assertEquals(expectedLtEq2, actualLtEq2);

    Predicate gt1 = new Predicate(">", attrAndValue);
    Expression expectedGt1 = Expressions.greaterThan(expectedTerm, value);
    Expression actualGt1 = SparkV2Filters.convert(gt1);
    assertEquals(expectedGt1, actualGt1);

    Predicate gt2 = new Predicate(">", valueAndAttr);
    Expression expectedGt2 = Expressions.lessThan(expectedTerm, value);
    Expression actualGt2 = SparkV2Filters.convert(gt2);
    assertEquals(expectedGt2, actualGt2);

    Predicate gtEq1 = new Predicate(">=", attrAndValue);
    Expression expectedGtEq1 = Expressions.greaterThanOrEqual(expectedTerm, value);
    Expression actualGtEq1 = SparkV2Filters.convert(gtEq1);
    assertEquals(expectedGtEq1, actualGtEq1);

    Predicate gtEq2 = new Predicate(">=", valueAndAttr);
    Expression expectedGtEq2 = Expressions.lessThanOrEqual(expectedTerm, value);
    Expression actualGtEq2 = SparkV2Filters.convert(gtEq2);
    assertEquals(expectedGtEq2, actualGtEq2);

    Predicate eq1 = new Predicate("=", attrAndValue);
    Expression expectedEq1 = Expressions.equal(expectedTerm, value);
    Expression actualEq1 = SparkV2Filters.convert(eq1);
    assertEquals(expectedEq1, actualEq1);

    Predicate eq2 = new Predicate("=", valueAndAttr);
    Expression expectedEq2 = Expressions.equal(expectedTerm, value);
    Expression actualEq2 = SparkV2Filters.convert(eq2);
    assertEquals(expectedEq2, actualEq2);

    Predicate notEq1 = new Predicate("<>", attrAndValue);
    Expression expectedNotEq1 = Expressions.notEqual(expectedTerm, value);
    Expression actualNotEq1 = SparkV2Filters.convert(notEq1);
    assertEquals(expectedNotEq1, actualNotEq1);

    Predicate notEq2 = new Predicate("<>", valueAndAttr);
    Expression expectedNotEq2 = Expressions.notEqual(expectedTerm, value);
    Expression actualNotEq2 = SparkV2Filters.convert(notEq2);
    assertEquals(expectedNotEq2, actualNotEq2);

    Predicate eqNullSafe1 = new Predicate("<=>", attrAndValue);
    Expression expectedEqNullSafe1 = Expressions.equal(expectedTerm, value);
    Expression actualEqNullSafe1 = SparkV2Filters.convert(eqNullSafe1);
    assertEquals(expectedEqNullSafe1, actualEqNullSafe1);

    Predicate eqNullSafe2 = new Predicate("<=>", valueAndAttr);
    Expression expectedEqNullSafe2 = Expressions.equal(expectedTerm, value);
    Expression actualEqNullSafe2 = SparkV2Filters.convert(eqNullSafe2);
    assertEquals(expectedEqNullSafe2, actualEqNullSafe2);

    Predicate in = new Predicate("IN", attrAndValue);
    Expression expectedIn = Expressions.in(expectedTerm, value);
    Expression actualIn = SparkV2Filters.convert(in);
    assertEquals(expectedIn, actualIn);

    Predicate notIn = new Not(in);
    Expression expectedNotIn =
        Expressions.and(Expressions.notNull(expectedTerm), Expressions.notIn(expectedTerm, value));
    Expression actualNotIn = SparkV2Filters.convert(notIn);
    assertEquals(expectedNotIn, actualNotIn);

    Predicate and = new And(lt1, eq1);
    Expression expectedAnd = Expressions.and(expectedLt1, expectedEq1);
    Expression actualAnd = SparkV2Filters.convert(and);
    assertEquals(expectedAnd, actualAnd);

    org.apache.spark.sql.connector.expressions.Expression[] attrAndAttr = expressions(udf, udf);
    Predicate invalid = new Predicate("<", attrAndAttr);
    Predicate andWithInvalidLeft = new And(invalid, eq1);
    Expression convertedAnd = SparkV2Filters.convert(andWithInvalidLeft);
    assertThat(convertedAnd).isNull();

    Predicate or = new Or(lt1, eq1);
    Expression expectedOr = Expressions.or(expectedLt1, expectedEq1);
    Expression actualOr = SparkV2Filters.convert(or);
    assertEquals(expectedOr, actualOr);

    Predicate orWithInvalidLeft = new Or(invalid, eq1);
    Expression convertedOr = SparkV2Filters.convert(orWithInvalidLeft);
    assertThat(convertedOr).isNull();

    Predicate not = new Not(lt1);
    Expression expectedNot = Expressions.not(expectedLt1);
    Expression actualNot = SparkV2Filters.convert(not);
    assertEquals(expectedNot, actualNot);
  }

  private static void assertEquals(Expression expected, Expression actual) {
    assertThat(ExpressionUtil.equivalent(expected, actual, STRUCT, true)).isTrue();
  }

  private org.apache.spark.sql.connector.expressions.Expression[] expressions(
      org.apache.spark.sql.connector.expressions.Expression... expressions) {
    return expressions;
  }

  private static int dateToYears(String dateString) {
    return DateTimeUtil.daysToYears(DateTimeUtil.isoDateToDays(dateString));
  }

  private static int timestampToYears(String timestampString) {
    return DateTimeUtil.microsToYears(DateTimeUtil.isoTimestamptzToMicros(timestampString));
  }

  private static int timestampNtzToYears(String timestampNtzString) {
    return DateTimeUtil.microsToYears(DateTimeUtil.isoTimestampToMicros(timestampNtzString));
  }

  private static int dateToMonths(String dateString) {
    return DateTimeUtil.daysToMonths(DateTimeUtil.isoDateToDays(dateString));
  }

  private static int timestampToMonths(String timestampString) {
    return DateTimeUtil.microsToMonths(DateTimeUtil.isoTimestamptzToMicros(timestampString));
  }

  private static int timestampNtzToMonths(String timestampNtzString) {
    return DateTimeUtil.microsToMonths(DateTimeUtil.isoTimestampToMicros(timestampNtzString));
  }

  private static int dateToDays(String dateString) {
    return DateTimeUtil.isoDateToDays(dateString);
  }

  private static int timestampToDays(String timestampString) {
    return DateTimeUtil.microsToDays(DateTimeUtil.isoTimestamptzToMicros(timestampString));
  }

  private static int timestampNtzToDays(String timestampNtzString) {
    return DateTimeUtil.microsToDays(DateTimeUtil.isoTimestampToMicros(timestampNtzString));
  }

  private static int timestampToHours(String timestampString) {
    return DateTimeUtil.microsToHours(DateTimeUtil.isoTimestamptzToMicros(timestampString));
  }

  private static int timestampNtzToHours(String timestampNtzString) {
    return DateTimeUtil.microsToHours(DateTimeUtil.isoTimestampToMicros(timestampNtzString));
  }
}
