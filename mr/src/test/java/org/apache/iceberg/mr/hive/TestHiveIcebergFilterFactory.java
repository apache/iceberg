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

package org.apache.iceberg.mr.hive;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.ZoneOffset;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.iceberg.expressions.And;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.expressions.Not;
import org.apache.iceberg.expressions.Or;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestHiveIcebergFilterFactory {

  @Test
  public void testEqualsOperand() {
    SearchArgument.Builder builder = SearchArgumentFactory.newBuilder();
    SearchArgument arg = builder.startAnd().equals("salary", PredicateLeaf.Type.LONG, 3000L).end().build();

    UnboundPredicate expected = Expressions.equal("salary", 3000L);
    UnboundPredicate actual = (UnboundPredicate) HiveIcebergFilterFactory.generateFilterExpression(arg);

    assertPredicatesMatch(expected, actual);
  }

  @Test
  public void testEqualsOperandRewrite() {
    SearchArgument.Builder builder = SearchArgumentFactory.newBuilder();
    SearchArgument arg = builder.startAnd().equals("float", PredicateLeaf.Type.FLOAT, Double.NaN).end().build();

    UnboundPredicate expected = Expressions.isNaN("float");
    UnboundPredicate actual = (UnboundPredicate) HiveIcebergFilterFactory.generateFilterExpression(arg);

    assertPredicatesMatch(expected, actual);
  }

  @Test
  public void testNotEqualsOperand() {
    SearchArgument.Builder builder = SearchArgumentFactory.newBuilder();
    SearchArgument arg = builder.startNot().equals("salary", PredicateLeaf.Type.LONG, 3000L).end().build();

    Not expected = (Not) Expressions.not(Expressions.equal("salary", 3000L));
    Not actual = (Not) HiveIcebergFilterFactory.generateFilterExpression(arg);

    UnboundPredicate childExpressionActual = (UnboundPredicate) actual.child();
    UnboundPredicate childExpressionExpected = Expressions.equal("salary", 3000L);

    assertEquals(actual.op(), expected.op());
    assertEquals(actual.child().op(), expected.child().op());
    assertEquals(childExpressionActual.ref().name(), childExpressionExpected.ref().name());
    assertEquals(childExpressionActual.literal(), childExpressionExpected.literal());
  }

  @Test
  public void testLessThanOperand() {
    SearchArgument.Builder builder = SearchArgumentFactory.newBuilder();
    SearchArgument arg = builder.startAnd().lessThan("salary", PredicateLeaf.Type.LONG, 3000L).end().build();

    UnboundPredicate expected = Expressions.lessThan("salary", 3000L);
    UnboundPredicate actual = (UnboundPredicate) HiveIcebergFilterFactory.generateFilterExpression(arg);

    assertEquals(actual.op(), expected.op());
    assertEquals(actual.literal(), expected.literal());
    assertEquals(actual.ref().name(), expected.ref().name());
  }

  @Test
  public void testLessThanEqualsOperand() {
    SearchArgument.Builder builder = SearchArgumentFactory.newBuilder();
    SearchArgument arg = builder.startAnd().lessThanEquals("salary", PredicateLeaf.Type.LONG, 3000L).end().build();

    UnboundPredicate expected = Expressions.lessThanOrEqual("salary", 3000L);
    UnboundPredicate actual = (UnboundPredicate) HiveIcebergFilterFactory.generateFilterExpression(arg);

    assertPredicatesMatch(expected, actual);
  }

  @Test
  public void testInOperand() {
    SearchArgument.Builder builder = SearchArgumentFactory.newBuilder();
    SearchArgument arg = builder.startAnd().in("salary", PredicateLeaf.Type.LONG, 3000L, 4000L).end().build();

    UnboundPredicate expected = Expressions.in("salary", 3000L, 4000L);
    UnboundPredicate actual = (UnboundPredicate) HiveIcebergFilterFactory.generateFilterExpression(arg);

    assertEquals(actual.op(), expected.op());
    assertEquals(actual.literals(), expected.literals());
    assertEquals(actual.ref().name(), expected.ref().name());
  }

  @Test
  public void testBetweenOperand() {
    SearchArgument.Builder builder = SearchArgumentFactory.newBuilder();
    SearchArgument arg = builder
        .startAnd()
        .between("salary", PredicateLeaf.Type.LONG, 3000L, 4000L).end().build();

    And expected = (And) Expressions.and(Expressions.greaterThanOrEqual("salary", 3000L),
        Expressions.lessThanOrEqual("salary", 3000L));
    And actual = (And) HiveIcebergFilterFactory.generateFilterExpression(arg);

    assertEquals(actual.op(), expected.op());
    assertEquals(actual.left().op(), expected.left().op());
    assertEquals(actual.right().op(), expected.right().op());
  }

  @Test
  public void testIsNullOperand() {
    SearchArgument.Builder builder = SearchArgumentFactory.newBuilder();
    SearchArgument arg = builder.startAnd().isNull("salary", PredicateLeaf.Type.LONG).end().build();

    UnboundPredicate expected = Expressions.isNull("salary");
    UnboundPredicate actual = (UnboundPredicate) HiveIcebergFilterFactory.generateFilterExpression(arg);

    assertEquals(actual.op(), expected.op());
    assertEquals(actual.ref().name(), expected.ref().name());
  }

  @Test
  public void testAndOperand() {
    SearchArgument.Builder builder = SearchArgumentFactory.newBuilder();
    SearchArgument arg = builder
        .startAnd()
        .equals("salary", PredicateLeaf.Type.LONG, 3000L)
        .equals("salary", PredicateLeaf.Type.LONG, 4000L)
        .end().build();

    And expected = (And) Expressions
        .and(Expressions.equal("salary", 3000L), Expressions.equal("salary", 4000L));
    And actual = (And) HiveIcebergFilterFactory.generateFilterExpression(arg);

    assertEquals(actual.op(), expected.op());
    assertEquals(actual.left().op(), expected.left().op());
    assertEquals(actual.right().op(), expected.right().op());
  }

  @Test
  public void testOrOperand() {
    SearchArgument.Builder builder = SearchArgumentFactory.newBuilder();
    SearchArgument arg = builder
        .startOr()
        .equals("salary", PredicateLeaf.Type.LONG, 3000L)
        .equals("salary", PredicateLeaf.Type.LONG, 4000L)
        .end().build();

    Or expected = (Or) Expressions
        .or(Expressions.equal("salary", 3000L), Expressions.equal("salary", 4000L));
    Or actual = (Or) HiveIcebergFilterFactory.generateFilterExpression(arg);

    assertEquals(actual.op(), expected.op());
    assertEquals(actual.left().op(), expected.left().op());
    assertEquals(actual.right().op(), expected.right().op());
  }

  @Test
  public void testStringType() {
    SearchArgument.Builder builder = SearchArgumentFactory.newBuilder();
    SearchArgument arg = builder.startAnd().equals("string", PredicateLeaf.Type.STRING, "Joe").end().build();

    UnboundPredicate expected = Expressions.equal("string", "Joe");
    UnboundPredicate actual = (UnboundPredicate) HiveIcebergFilterFactory.generateFilterExpression(arg);

    assertPredicatesMatch(expected, actual);
  }

  @Test
  public void testFloatType() {
    SearchArgument.Builder builder = SearchArgumentFactory.newBuilder();
    SearchArgument arg = builder.startAnd().equals("float", PredicateLeaf.Type.FLOAT, 1200D).end().build();

    UnboundPredicate expected = Expressions.equal("float", 1200D);
    UnboundPredicate actual = (UnboundPredicate) HiveIcebergFilterFactory.generateFilterExpression(arg);

    assertPredicatesMatch(expected, actual);
  }

  @Test
  public void testBooleanType() {
    SearchArgument.Builder builder = SearchArgumentFactory.newBuilder();
    SearchArgument arg = builder.startAnd().equals("boolean", PredicateLeaf.Type.BOOLEAN, true).end().build();

    UnboundPredicate expected = Expressions.equal("boolean", true);
    UnboundPredicate actual = (UnboundPredicate) HiveIcebergFilterFactory.generateFilterExpression(arg);

    assertPredicatesMatch(expected, actual);
  }

  @Test
  public void testDateType() {
    SearchArgument.Builder builder = SearchArgumentFactory.newBuilder();
    Date gmtDate = new Date(LocalDate.of(2015, 11, 12).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli());
    SearchArgument arg = builder.startAnd().equals("date", PredicateLeaf.Type.DATE, gmtDate).end().build();

    UnboundPredicate expected = Expressions.equal("date", Literal.of("2015-11-12").to(Types.DateType.get()).value());
    UnboundPredicate actual = (UnboundPredicate) HiveIcebergFilterFactory.generateFilterExpression(arg);

    assertPredicatesMatch(expected, actual);
  }

  @Test
  public void testTimestampType() {
    Literal<Long> timestampLiteral = Literal.of("2012-10-02T05:16:17.123456").to(Types.TimestampType.withoutZone());
    long timestampMicros = timestampLiteral.value();
    Timestamp ts = Timestamp.from(DateTimeUtil.timestampFromMicros(timestampMicros).toInstant(ZoneOffset.UTC));

    SearchArgument.Builder builder = SearchArgumentFactory.newBuilder();
    SearchArgument arg = builder.startAnd().equals("timestamp", PredicateLeaf.Type.TIMESTAMP, ts).end().build();

    UnboundPredicate expected = Expressions.equal("timestamp", timestampMicros);
    UnboundPredicate actual = (UnboundPredicate) HiveIcebergFilterFactory.generateFilterExpression(arg);

    assertPredicatesMatch(expected, actual);
  }

  @Test
  public void testDecimalType() {
    SearchArgument.Builder builder = SearchArgumentFactory.newBuilder();
    SearchArgument arg = builder.startAnd().equals("decimal", PredicateLeaf.Type.DECIMAL,
            new HiveDecimalWritable("20.12")).end().build();

    UnboundPredicate expected = Expressions.equal("decimal", new BigDecimal("20.12"));
    UnboundPredicate actual = (UnboundPredicate) HiveIcebergFilterFactory.generateFilterExpression(arg);

    assertPredicatesMatch(expected, actual);
  }

  private void assertPredicatesMatch(UnboundPredicate expected, UnboundPredicate actual) {
    assertEquals(expected.op(), actual.op());
    assertEquals(expected.literal(), actual.literal());
    assertEquals(expected.ref().name(), actual.ref().name());
  }
}
