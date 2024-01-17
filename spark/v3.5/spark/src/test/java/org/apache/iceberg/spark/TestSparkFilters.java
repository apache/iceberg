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

import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.source.DummyBroadcastedJoinKeysWrapper;
import org.apache.iceberg.spark.source.Tuple;
import org.apache.iceberg.spark.source.broadcastvar.BroadcastHRUnboundPredicate;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.catalyst.bcvar.BroadcastedJoinKeysWrapper;
import org.apache.spark.sql.sources.And;
import org.apache.spark.sql.sources.EqualNullSafe;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.GreaterThanOrEqual;
import org.apache.spark.sql.sources.In;
import org.apache.spark.sql.sources.IsNotNull;
import org.apache.spark.sql.sources.IsNull;
import org.apache.spark.sql.sources.LessThan;
import org.apache.spark.sql.sources.LessThanOrEqual;
import org.apache.spark.sql.sources.Not;

import org.junit.jupiter.api.Test;

import org.apache.spark.sql.types.DataTypes;

import static org.apache.iceberg.types.Types.NestedField.required;

public class TestSparkFilters {

  @Test
  public void testQuotedAttributes() {
    Map<String, String> attrMap = Maps.newHashMap();
    attrMap.put("id", "id");
    attrMap.put("`i.d`", "i.d");
    attrMap.put("`i``d`", "i`d");
    attrMap.put("`d`.b.`dd```", "d.b.dd`");
    attrMap.put("a.`aa```.c", "a.aa`.c");

    attrMap.forEach(
        (quoted, unquoted) -> {
          IsNull isNull = IsNull.apply(quoted);
          Expression expectedIsNull = Expressions.isNull(unquoted);

          Tuple<Boolean, Expression> actualIsNullTuple = SparkFilters.convert(isNull);
          Assert.assertEquals(
              "IsNull must match",
              expectedIsNull.toString(),
              actualIsNullTuple.getElement2().toString());

          IsNotNull isNotNull = IsNotNull.apply(quoted);
          Expression expectedIsNotNull = Expressions.notNull(unquoted);
          Tuple<Boolean, Expression> actualIsNotNullTuple = SparkFilters.convert(isNotNull);
          Assert.assertEquals(
              "IsNotNull must match",
              expectedIsNotNull.toString(),
              actualIsNotNullTuple.getElement2().toString());

          LessThan lt = LessThan.apply(quoted, 1);
          Expression expectedLt = Expressions.lessThan(unquoted, 1);
          Tuple<Boolean, Expression> actualLtTup = SparkFilters.convert(lt);
          Assert.assertEquals(
              "LessThan must match", expectedLt.toString(), actualLtTup.getElement2().toString());

          LessThanOrEqual ltEq = LessThanOrEqual.apply(quoted, 1);
          Expression expectedLtEq = Expressions.lessThanOrEqual(unquoted, 1);
          Tuple<Boolean, Expression> actualLtEqTup = SparkFilters.convert(ltEq);
          Assert.assertEquals(
              "LessThanOrEqual must match",
              expectedLtEq.toString(),
              actualLtEqTup.getElement2().toString());

          GreaterThan gt = GreaterThan.apply(quoted, 1);
          Expression expectedGt = Expressions.greaterThan(unquoted, 1);
          Tuple<Boolean, Expression> actualGtTp = SparkFilters.convert(gt);
          Assert.assertEquals(
              "GreaterThan must match", expectedGt.toString(), actualGtTp.getElement2().toString());

          GreaterThanOrEqual gtEq = GreaterThanOrEqual.apply(quoted, 1);
          Expression expectedGtEq = Expressions.greaterThanOrEqual(unquoted, 1);
          Tuple<Boolean, Expression> actualGtEqTp = SparkFilters.convert(gtEq);
          Assert.assertEquals(
              "GreaterThanOrEqual must match",
              expectedGtEq.toString(),
              actualGtEqTp.getElement2().toString());

          EqualTo eq = EqualTo.apply(quoted, 1);
          Expression expectedEq = Expressions.equal(unquoted, 1);
          Tuple<Boolean, Expression> actualEqTp = SparkFilters.convert(eq);
          Assert.assertEquals(
              "EqualTo must match", expectedEq.toString(), actualEqTp.getElement2().toString());

          EqualNullSafe eqNullSafe = EqualNullSafe.apply(quoted, 1);
          Expression expectedEqNullSafe = Expressions.equal(unquoted, 1);
          Tuple<Boolean, Expression> actualEqNullSafeTp = SparkFilters.convert(eqNullSafe);
          Assert.assertEquals(
              "EqualNullSafe must match",
              expectedEqNullSafe.toString(),
              actualEqNullSafeTp.getElement2().toString());

          In in = In.apply(quoted, new Integer[] {1});
          Expression expectedIn = Expressions.in(unquoted, 1);
          Tuple<Boolean, Expression> actualInTp = SparkFilters.convert(in);
          Assert.assertEquals(
              "In must match", expectedIn.toString(), actualInTp.getElement2().toString());

          // test range-in
          Schema schema = new Schema(required(100, unquoted, Types.IntegerType.get()));
          BroadcastedJoinKeysWrapper dummyWrapper =
              new DummyBroadcastedJoinKeysWrapper(DataTypes.IntegerType, new Object[] {1}, 1L);
          In rangeIn = In.apply(quoted, new Object[] {dummyWrapper});
          BroadcastHRUnboundPredicate expectedRangeIn =
              new BroadcastHRUnboundPredicate<>(unquoted, dummyWrapper);
          Tuple<Boolean, Expression> actualRangeInTup = SparkFilters.convert(rangeIn, schema);
          Assert.assertEquals(
              "Range In must match",
              expectedRangeIn.toStringWithData(),
              ((BroadcastHRUnboundPredicate) actualRangeInTup.getElement2()).toStringWithData());
>>>>>>> broadcastvar-push
        });
  }

  @Test
  public void testTimestampFilterConversion() {
    Instant instant = Instant.parse("2018-10-18T00:00:57.907Z");
    Timestamp timestamp = Timestamp.from(instant);
    long epochMicros = ChronoUnit.MICROS.between(Instant.EPOCH, instant);

    Tuple<Boolean, Expression> instantExpressionTp =
        SparkFilters.convert(GreaterThan.apply("x", instant));
    Tuple<Boolean, Expression> timestampExpressionTp =
        SparkFilters.convert(GreaterThan.apply("x", timestamp));
    Expression rawExpression = Expressions.greaterThan("x", epochMicros);

    Assert.assertEquals(
        "Generated Timestamp expression should be correct",
        rawExpression.toString(),
        timestampExpressionTp.getElement2().toString());
    Assert.assertEquals(
        "Generated Instant expression should be correct",
        rawExpression.toString(),
        instantExpressionTp.getElement2().toString());
  }

  @Test
  public void testTimestampFilterConversionForRangeIn() {
    Instant instant = Instant.parse("2018-10-18T00:00:57.907Z");
    Timestamp timestamp = Timestamp.from(instant);
    long epochMicros = ChronoUnit.MICROS.between(Instant.EPOCH, instant);

    Schema schema = new Schema(required(100, "x", Types.TimestampType.withZone()));

    BroadcastedJoinKeysWrapper dummyWrapper1 =
        new DummyBroadcastedJoinKeysWrapper(DataTypes.TimestampType, new Object[] {instant}, 1L);
    Tuple<Boolean, Expression> instantExpressionTp =
        SparkFilters.convert(In.apply("x", new Object[] {dummyWrapper1}), schema);

    BroadcastedJoinKeysWrapper dummyWrapper2 =
        new DummyBroadcastedJoinKeysWrapper(DataTypes.TimestampType, new Object[] {timestamp}, 1L);
    Tuple<Boolean, Expression> timestampExpressionTp =
        SparkFilters.convert(In.apply("x", new Object[] {dummyWrapper2}), schema);

    BroadcastedJoinKeysWrapper dummyWrapper3 =
        new DummyBroadcastedJoinKeysWrapper(
            DataTypes.TimestampType, new Object[] {epochMicros}, 1L);
    BroadcastHRUnboundPredicate rawExpression =
        new BroadcastHRUnboundPredicate<>("x", dummyWrapper3);

    Assert.assertEquals(
        "Generated Timestamp expression should be correct",
        rawExpression.toStringWithData(),
        ((BroadcastHRUnboundPredicate) timestampExpressionTp.getElement2()).toStringWithData());
    Assert.assertEquals(
        "Generated Instant expression should be correct",
        rawExpression.toStringWithData(),
        ((BroadcastHRUnboundPredicate) instantExpressionTp.getElement2()).toStringWithData());
  }
  @Test
  public void testLocalDateTimeFilterConversion() {
    LocalDateTime ldt = LocalDateTime.parse("2018-10-18T00:00:57");
    long epochMicros =
        ChronoUnit.MICROS.between(LocalDateTime.ofInstant(Instant.EPOCH, ZoneId.of("UTC")), ldt);

    Tuple<Boolean, Expression> instantExpressionTuple =
        SparkFilters.convert(GreaterThan.apply("x", ldt));
    Expression rawExpression = Expressions.greaterThan("x", epochMicros);

    Assert.assertEquals(
        "Generated Instant expression should be correct",
        rawExpression.toString(),
        instantExpressionTuple.getElement2().toString());
  }

  @Test
  public void testDateFilterConversion() {
    LocalDate localDate = LocalDate.parse("2018-10-18");
    Date date = Date.valueOf(localDate);
    long epochDay = localDate.toEpochDay();

    Tuple<Boolean, Expression> localDateExpressionTp =
        SparkFilters.convert(GreaterThan.apply("x", localDate));
    Tuple<Boolean, Expression> dateExpressionTp =
        SparkFilters.convert(GreaterThan.apply("x", date));
    Expression rawExpression = Expressions.greaterThan("x", epochDay);

    Assert.assertEquals(
        "Generated localdate expression should be correct",
        rawExpression.toString(),
        localDateExpressionTp.getElement2().toString());

    Assert.assertEquals(
        "Generated date expression should be correct",
        rawExpression.toString(),
        dateExpressionTp.getElement2().toString());
  }

  @Test
  public void testDateFilterConversionForRangeIn() {
    LocalDate localDate = LocalDate.parse("2018-10-18");
    Date date = Date.valueOf(localDate);
    long epochDay = localDate.toEpochDay();

    Schema schema = new Schema(required(100, "x", Types.DateType.get()));

    BroadcastedJoinKeysWrapper dummyWrapper1 =
        new DummyBroadcastedJoinKeysWrapper(DataTypes.DateType, new Object[] {localDate}, 1L);
    Tuple<Boolean, Expression> localDateExpressionTp =
        SparkFilters.convert(In.apply("x", new Object[] {dummyWrapper1}), schema);

    BroadcastedJoinKeysWrapper dummyWrapper2 =
        new DummyBroadcastedJoinKeysWrapper(DataTypes.DateType, new Object[] {date}, 1L);
    Tuple<Boolean, Expression> dateExpressionTp =
        SparkFilters.convert(In.apply("x", new Object[] {dummyWrapper2}), schema);

    BroadcastedJoinKeysWrapper dummyWrapper3 =
        new DummyBroadcastedJoinKeysWrapper(DataTypes.DateType, new Object[] {epochDay}, 1L);
    BroadcastHRUnboundPredicate rawExpression =
        new BroadcastHRUnboundPredicate<>("x", dummyWrapper3);

    Assert.assertEquals(
        "Generated localdate expression should be correct",
        rawExpression.toStringWithData(),
        ((BroadcastHRUnboundPredicate) localDateExpressionTp.getElement2()).toStringWithData());

    Assert.assertEquals(
        "Generated date expression should be correct",
        rawExpression.toStringWithData(),
        ((BroadcastHRUnboundPredicate) dateExpressionTp.getElement2()).toStringWithData());
  }

  @Test
  public void testNestedInInsideNot() {
    Not filter =
        Not.apply(And.apply(EqualTo.apply("col1", 1), In.apply("col2", new Integer[] {1, 2})));
    Tuple<Boolean, Expression> converted = SparkFilters.convert(filter);
    Assert.assertNull("Expression should not be converted", converted);
  }

  @Test
  public void testNotIn() {
    Not filter = Not.apply(In.apply("col", new Integer[] {1, 2}));
    Tuple<Boolean, Expression> actualTp = SparkFilters.convert(filter);
    Expression expected =
        Expressions.and(Expressions.notNull("col"), Expressions.notIn("col", 1, 2));
    Assert.assertEquals(
        "Expressions should match", expected.toString(), actualTp.getElement2().toString());
  }
}
