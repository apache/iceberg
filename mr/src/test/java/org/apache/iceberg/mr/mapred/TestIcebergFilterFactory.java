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

package org.apache.iceberg.mr.mapred;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.iceberg.expressions.And;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Not;
import org.apache.iceberg.expressions.Or;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestIcebergFilterFactory {

  @Test
  public void testEqualsOperand() {
    SearchArgument.Builder builder = SearchArgumentFactory.newBuilder();
    SearchArgument arg = builder.startAnd().equals("salary", PredicateLeaf.Type.LONG, 3000L).end().build();

    UnboundPredicate expected = Expressions.equal("salary", 3000L);
    UnboundPredicate actual = (UnboundPredicate) IcebergFilterFactory.generateFilterExpression(arg);

    assertEquals(actual.op(), expected.op());
    assertEquals(actual.literal(), expected.literal());
    assertEquals(actual.ref().name(), expected.ref().name());
  }

  @Test
  public void testNotEqualsOperand() {
    SearchArgument.Builder builder = SearchArgumentFactory.newBuilder();
    SearchArgument arg = builder.startNot().equals("salary", PredicateLeaf.Type.LONG, 3000L).end().build();

    Not expected = (Not) Expressions.not(Expressions.equal("salary", 3000L));
    Not actual = (Not) IcebergFilterFactory.generateFilterExpression(arg);

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
    UnboundPredicate actual = (UnboundPredicate) IcebergFilterFactory.generateFilterExpression(arg);

    assertEquals(actual.op(), expected.op());
    assertEquals(actual.literal(), expected.literal());
    assertEquals(actual.ref().name(), expected.ref().name());
  }

  @Test
  public void testLessThanEqualsOperand() {
    SearchArgument.Builder builder = SearchArgumentFactory.newBuilder();
    SearchArgument arg = builder.startAnd().lessThanEquals("salary", PredicateLeaf.Type.LONG, 3000L).end().build();

    UnboundPredicate expected = Expressions.lessThanOrEqual("salary", 3000L);
    UnboundPredicate actual = (UnboundPredicate) IcebergFilterFactory.generateFilterExpression(arg);

    assertEquals(actual.op(), expected.op());
    assertEquals(actual.literal(), expected.literal());
    assertEquals(actual.ref().name(), expected.ref().name());
  }

  @Test
  public void testInOperandWithLong() {
    SearchArgument.Builder builder = SearchArgumentFactory.newBuilder();
    SearchArgument arg = builder.startAnd().in("salary", PredicateLeaf.Type.LONG, 3000L, 4000L).end().build();

    UnboundPredicate expected = Expressions.in("salary", 3000L, 4000L);
    UnboundPredicate actual = (UnboundPredicate) IcebergFilterFactory.generateFilterExpression(arg);

    assertEquals(actual.op(), expected.op());
    assertEquals(actual.literals(), expected.literals());
    assertEquals(actual.ref().name(), expected.ref().name());
  }

  @Test
  public void testInOperandWithDecimal() {
    SearchArgument.Builder builder = SearchArgumentFactory.newBuilder();
    SearchArgument arg = builder.startAnd().in("decimal", PredicateLeaf.Type.DECIMAL,
        new HiveDecimalWritable("12.14"), new HiveDecimalWritable("13.15")).end().build();

    UnboundPredicate expected = Expressions.in("decimal", BigDecimal.valueOf(12.14), BigDecimal.valueOf(13.15));
    UnboundPredicate actual = (UnboundPredicate) IcebergFilterFactory.generateFilterExpression(arg);

    assertEquals(actual.op(), expected.op());
    assertEquals(actual.literals(), expected.literals());
    assertEquals(actual.ref().name(), expected.ref().name());
  }

  @Test
  public void testInOperandWithDate() {
    SearchArgument.Builder builder = SearchArgumentFactory.newBuilder();
    SearchArgument arg = builder
        .startAnd()
        .in("date", PredicateLeaf.Type.DATE,
            Date.valueOf("2020-06-15"), Date.valueOf("2021-06-15"))
        .end()
        .build();

    UnboundPredicate expected = Expressions.in("date", LocalDate.of(2020, 6, 15).toEpochDay(),
        LocalDate.of(2021, 6, 15).toEpochDay());
    UnboundPredicate actual = (UnboundPredicate) IcebergFilterFactory.generateFilterExpression(arg);

    assertEquals(actual.op(), expected.op());
    assertEquals(actual.literals(), expected.literals());
    assertEquals(actual.ref().name(), expected.ref().name());
    assertEquals(expected.toString(), actual.toString());
  }

  @Test
  public void testInOperandWithTimestamp() {
    Timestamp timestampHiveFilterOne = Timestamp.valueOf("2016-11-16 06:43:19.77");
    Timestamp timestampHiveFilterTwo = Timestamp.valueOf("2017-11-16 06:43:19.77");

    SearchArgument.Builder builder = SearchArgumentFactory.newBuilder();
    SearchArgument arg = builder
        .startAnd()
        .in("timestamp", PredicateLeaf.Type.TIMESTAMP, timestampHiveFilterOne, timestampHiveFilterTwo)
        .end()
        .build();

    UnboundPredicate expected = Expressions.in("timestamp",
        1479278599770000L, 1510814599770000L);
    UnboundPredicate actual = (UnboundPredicate) IcebergFilterFactory.generateFilterExpression(arg);

    assertEquals(expected.op(), actual.op());
    assertEquals(expected.literals(), actual.literals());
    assertEquals(expected.ref().name(), actual.ref().name());
    assertEquals(expected.toString(), actual.toString());
  }


  @Test
  public void testBetweenOperand() {
    SearchArgument.Builder builder = SearchArgumentFactory.newBuilder();
    SearchArgument arg = builder
            .startAnd()
            .between("salary", PredicateLeaf.Type.LONG, 3000L, 4000L).end().build();

    And expected = (And) Expressions.and(Expressions.greaterThanOrEqual("salary", 3000L),
            Expressions.lessThanOrEqual("salary", 3000L));
    And actual = (And) IcebergFilterFactory.generateFilterExpression(arg);

    assertEquals(actual.op(), expected.op());
    assertEquals(actual.left().op(), expected.left().op());
    assertEquals(actual.right().op(), expected.right().op());
  }

  @Test
  public void testIsNullOperand() {
    SearchArgument.Builder builder = SearchArgumentFactory.newBuilder();
    SearchArgument arg = builder.startAnd().isNull("salary", PredicateLeaf.Type.LONG).end().build();

    UnboundPredicate expected = Expressions.isNull("salary");
    UnboundPredicate actual = (UnboundPredicate) IcebergFilterFactory.generateFilterExpression(arg);

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
    And actual = (And) IcebergFilterFactory.generateFilterExpression(arg);

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
    Or actual = (Or) IcebergFilterFactory.generateFilterExpression(arg);

    assertEquals(actual.op(), expected.op());
    assertEquals(actual.left().op(), expected.left().op());
    assertEquals(actual.right().op(), expected.right().op());
  }

  @Test
  public void testManyAndOperand() {
    SearchArgument.Builder builder = SearchArgumentFactory.newBuilder();
    SearchArgument arg = builder
            .startAnd()
            .equals("salary", PredicateLeaf.Type.LONG, 3000L)
            .equals("job", PredicateLeaf.Type.LONG, 4000L)
            .equals("name", PredicateLeaf.Type.LONG, 9000L)
            .end()
            .build();

    And expected = (And) Expressions.and(
            Expressions.equal("salary", 3000L),
            Expressions.equal("job", 4000L),
            Expressions.equal("name", 9000L));

    And actual = (And) IcebergFilterFactory.generateFilterExpression(arg);

    assertEquals(actual.op(), expected.op());
    assertEquals(actual.right().op(), expected.right().op());
    assertEquals(actual.left().op(), expected.left().op());
  }

  @Test
  public void testManyOrOperand() {
    SearchArgument.Builder builder = SearchArgumentFactory.newBuilder();
    SearchArgument arg = builder
        .startOr()
        .equals("salary", PredicateLeaf.Type.LONG, 3000L)
        .equals("job", PredicateLeaf.Type.LONG, 4000L)
        .equals("name", PredicateLeaf.Type.LONG, 9000L)
        .end()
        .build();

    Or expected = (Or) Expressions.or(Expressions.or(Expressions.equal("salary", 3000L),
        Expressions.equal("job", 4000L)), Expressions.equal("name", 9000L));

    Or actual = (Or) IcebergFilterFactory.generateFilterExpression(arg);

    assertEquals(actual.op(), expected.op());
    assertEquals(actual.right().op(), expected.right().op());
    assertEquals(actual.left().op(), expected.left().op());
  }

  @Test
  public void testNestedFilter() {
    SearchArgument.Builder builder = SearchArgumentFactory.newBuilder();
    SearchArgument arg = builder
        .startOr()
        .equals("job", PredicateLeaf.Type.STRING, "dev")
        .startAnd()
        .equals("id", PredicateLeaf.Type.LONG, 3L)
        .equals("dept", PredicateLeaf.Type.STRING, "300")
        .end()
        .end()
        .build();

    And expected = (And) Expressions.and(Expressions.or(Expressions.equal("job", "dev"), Expressions.equal(
        "id", 3L)), Expressions.or(Expressions.equal("job", "dev"), Expressions.equal("dept", "300")));
    And actual = (And) IcebergFilterFactory.generateFilterExpression(arg);
    assertEquals(actual.op(), expected.op());
    assertEquals(actual.right().op(), expected.right().op());
    assertEquals(actual.left().op(), expected.left().op());
  }

  @Test
  public void testTypeConversion() {
    SearchArgument.Builder builder = SearchArgumentFactory.newBuilder();
    SearchArgument arg = builder
        .startAnd()
        .equals("date", PredicateLeaf.Type.DATE, Date.valueOf("2020-06-15"))
        .equals("timestamp", PredicateLeaf.Type.TIMESTAMP, Timestamp.valueOf("2016-11-16 06:43:19.77"))
        .equals("decimal", PredicateLeaf.Type.DECIMAL, new HiveDecimalWritable("12.12"))
        .equals("string", PredicateLeaf.Type.STRING, "hello world")
        .equals("long", PredicateLeaf.Type.LONG, 3020L)
        .equals("float", PredicateLeaf.Type.FLOAT, 4400D)
        .equals("boolean", PredicateLeaf.Type.BOOLEAN, true)
        .end()
        .build();

    Timestamp timestamp = Timestamp.valueOf("2016-11-16 06:43:19.77");
    And expected = (And) Expressions.and(
        Expressions.equal("date", LocalDate.of(2020, 6, 15).toEpochDay()),
        Expressions.equal("timestamp", 1479278599770000L),
        Expressions.equal("decimal", BigDecimal.valueOf(12.12)),
        Expressions.equal("string", "hello world"),
        Expressions.equal("long", 3020L),
        Expressions.equal("float", 4400D),
        Expressions.equal("boolean", true));

    And actual = (And) IcebergFilterFactory.generateFilterExpression(arg);

    assertEquals(expected.toString(), actual.toString());
    assertEquals(expected.op(), actual.op());

  }
}
