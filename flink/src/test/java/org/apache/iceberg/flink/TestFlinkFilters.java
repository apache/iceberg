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

package org.apache.iceberg.flink;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.expressions.ApiExpressionUtils;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.UnresolvedCallExpression;
import org.apache.flink.table.expressions.UnresolvedReferenceExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.expressions.utils.ApiExpressionDefaultVisitor;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.iceberg.expressions.And;
import org.apache.iceberg.expressions.Not;
import org.apache.iceberg.expressions.Or;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.util.DateTimeUtil;
import org.junit.Test;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;
import static org.junit.Assert.assertEquals;

public class TestFlinkFilters {

  private static final TableSchema TABLE_SCHEMA = TableSchema.builder()
      .field("field1", DataTypes.INT())
      .field("field2", DataTypes.BIGINT())
      .field("field3", DataTypes.FLOAT())
      .field("field4", DataTypes.DOUBLE())
      .field("field5", DataTypes.STRING())
      .field("field6", DataTypes.BOOLEAN())
      .field("field7", DataTypes.BINARY(10))
      .field("field8", DataTypes.DECIMAL(10, 2))
      .field("field9", DataTypes.DATE())
      .field("field10", DataTypes.TIME())
      .field("field11", DataTypes.TIMESTAMP())
      .field("field12", DataTypes.TIMESTAMP_WITH_TIME_ZONE())
      .field("field13", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE())
      .build();

  @Test
  public void testFlinkDataTypeEqual() {
    matchLiteral("field1", 1, 1);
    matchLiteral("field2", 10L, 10L);
    matchLiteral("field3", 1.2F, 1.2F);
    matchLiteral("field4", 3.4D, 3.4D);
    matchLiteral("field5", "abcd", "abcd");
    matchLiteral("field6", true, true);
    matchLiteral("field7", new byte[] {'a', 'b'}, new byte[] {'a', 'b'});
    matchLiteral("field8", BigDecimal.valueOf(10), BigDecimal.valueOf(10));

    LocalDate date = LocalDate.parse("2020-12-23");
    matchLiteral("field9", date, DateTimeUtil.daysFromDate(date));

    LocalTime time = LocalTime.parse("12:13:14");
    matchLiteral("field10", time, DateTimeUtil.microsFromTime(time));

    LocalDateTime dateTime = LocalDateTime.parse("2020-12-23T12:13:14");
    matchLiteral("field11", dateTime, DateTimeUtil.microsFromTimestamp(dateTime));

    Instant instant = Instant.parse("2020-12-23T12:13:14.00Z");
    matchLiteral("field12", instant, DateTimeUtil.microsFromInstant(instant));

    matchLiteral("field13", instant, DateTimeUtil.microsFromInstant(instant));
  }

  @Test
  public void testEquals() {
    UnboundPredicate<Integer> expected = org.apache.iceberg.expressions.Expressions.equal("field1", 1);

    org.apache.iceberg.expressions.Expression actual =
        FlinkFilters.convert(resolve(Expressions.$("field1").isEqual(Expressions.lit(1)))).get();
    assertPredicatesMatch(expected, (UnboundPredicate) actual);

    org.apache.iceberg.expressions.Expression actual1 =
        FlinkFilters.convert(resolve(Expressions.lit(1).isEqual(Expressions.$("field1")))).get();
    assertPredicatesMatch(expected, (UnboundPredicate) actual1);
  }

  @Test
  public void testNotEquals() {
    UnboundPredicate<Integer> expected = org.apache.iceberg.expressions.Expressions.notEqual("field1", 1);

    org.apache.iceberg.expressions.Expression actual =
        FlinkFilters.convert(resolve(Expressions.$("field1").isNotEqual(Expressions.lit(1)))).get();
    assertPredicatesMatch(expected, (UnboundPredicate) actual);

    org.apache.iceberg.expressions.Expression actual1 =
        FlinkFilters.convert(resolve(Expressions.lit(1).isNotEqual(Expressions.$("field1")))).get();
    assertPredicatesMatch(expected, (UnboundPredicate) actual1);
  }

  @Test
  public void testGreaterThan() {
    UnboundPredicate<Integer> expected = org.apache.iceberg.expressions.Expressions.greaterThan("field1", 1);

    org.apache.iceberg.expressions.Expression actual =
        FlinkFilters.convert(resolve(Expressions.$("field1").isGreater(Expressions.lit(1)))).get();
    assertPredicatesMatch(expected, (UnboundPredicate) actual);

    org.apache.iceberg.expressions.Expression actual1 =
        FlinkFilters.convert(resolve(Expressions.lit(1).isLess(Expressions.$("field1")))).get();
    assertPredicatesMatch(expected, (UnboundPredicate) actual1);
  }

  @Test
  public void testGreaterThanEquals() {
    UnboundPredicate<Integer> expected = org.apache.iceberg.expressions.Expressions.greaterThanOrEqual("field1", 1);

    org.apache.iceberg.expressions.Expression actual =
        FlinkFilters.convert(resolve(Expressions.$("field1").isGreaterOrEqual(Expressions.lit(1)))).get();
    assertPredicatesMatch(expected, (UnboundPredicate) actual);

    org.apache.iceberg.expressions.Expression actual1 =
        FlinkFilters.convert(resolve(Expressions.lit(1).isLessOrEqual(Expressions.$("field1")))).get();
    assertPredicatesMatch(expected, (UnboundPredicate) actual1);
  }

  @Test
  public void testLessThan() {
    UnboundPredicate<Integer> expected = org.apache.iceberg.expressions.Expressions.lessThan("field1", 1);

    org.apache.iceberg.expressions.Expression actual =
        FlinkFilters.convert(resolve(Expressions.$("field1").isLess(Expressions.lit(1)))).get();
    assertPredicatesMatch(expected, (UnboundPredicate) actual);

    org.apache.iceberg.expressions.Expression actual1 =
        FlinkFilters.convert(resolve(Expressions.lit(1).isGreater(Expressions.$("field1")))).get();
    assertPredicatesMatch(expected, (UnboundPredicate) actual1);
  }

  @Test
  public void testLessThanEquals() {
    UnboundPredicate<Integer> expected = org.apache.iceberg.expressions.Expressions.lessThanOrEqual("field1", 1);

    org.apache.iceberg.expressions.Expression actual =
        FlinkFilters.convert(resolve(Expressions.$("field1").isLessOrEqual(Expressions.lit(1)))).get();
    assertPredicatesMatch(expected, (UnboundPredicate) actual);

    org.apache.iceberg.expressions.Expression actual1 =
        FlinkFilters.convert(resolve(Expressions.lit(1).isGreaterOrEqual(Expressions.$("field1")))).get();
    assertPredicatesMatch(expected, (UnboundPredicate) actual1);
  }

  @Test
  public void testIsNull() {
    Expression expr = resolve($("field1").isNull());
    UnboundPredicate actual = (UnboundPredicate) FlinkFilters.convert(expr).get();
    UnboundPredicate<Object> expected = org.apache.iceberg.expressions.Expressions.isNull("field1");
    assertPredicatesMatch(expected, actual);
  }

  @Test
  public void testIsNotNull() {
    Expression expr = resolve($("field1").isNotNull());
    UnboundPredicate actual = (UnboundPredicate) FlinkFilters.convert(expr).get();
    UnboundPredicate<Object> expected = org.apache.iceberg.expressions.Expressions.notNull("field1");
    assertPredicatesMatch(expected, actual);
  }

  @Test
  public void testAnd() {
    Expression expr = resolve(Expressions.$("field1").isEqual(lit(1)).and(Expressions.$("field2").isEqual(lit(2L))));
    And actual = (And) FlinkFilters.convert(expr).get();
    And expected = (And) org.apache.iceberg.expressions.Expressions.and(
        org.apache.iceberg.expressions.Expressions.equal("field1", 1),
        org.apache.iceberg.expressions.Expressions.equal("field2", 2L));

    assertEquals(expected.op(), actual.op());
    assertEquals(expected.left().op(), actual.left().op());
    assertEquals(expected.right().op(), actual.right().op());
  }

  @Test
  public void testOr() {
    Expression expr = resolve($("field1").isEqual(lit(1)).or($("field2").isEqual(lit(2L))));
    Or actual = (Or) FlinkFilters.convert(expr).get();
    Or expected = (Or) org.apache.iceberg.expressions.Expressions.or(
        org.apache.iceberg.expressions.Expressions.equal("field1", 1),
        org.apache.iceberg.expressions.Expressions.equal("field2", 2L));

    assertEquals(expected.op(), actual.op());
    assertEquals(expected.left().op(), actual.left().op());
    assertEquals(expected.right().op(), actual.right().op());
  }

  @Test
  public void testNot() {
    Expression expr = resolve(ApiExpressionUtils.unresolvedCall(
        BuiltInFunctionDefinitions.NOT, $("field1").isEqual(lit(1))));
    Not actual = (Not) FlinkFilters.convert(expr).get();
    Not expected = (Not) org.apache.iceberg.expressions.Expressions.not(
        org.apache.iceberg.expressions.Expressions.equal("field1", 1));

    assertEquals(expected.op(), actual.op());
    assertPredicatesMatch((UnboundPredicate) expected.child(), (UnboundPredicate) actual.child());
  }

  @Test
  public void testLike() {
    UnboundPredicate expected = org.apache.iceberg.expressions.Expressions.startsWith("field5", "abc");
    Expression expr = resolve(ApiExpressionUtils.unresolvedCall(
        BuiltInFunctionDefinitions.LIKE, $("field5"), lit("abc%")));
    assertPredicatesMatch(expected, (UnboundPredicate) FlinkFilters.convert(expr).get());
  }

  private void matchLiteral(String fieldName, Object flinkLiteral, Object icebergLiteral) {
    Expression expr = resolve(Expressions.$(fieldName).isEqual(Expressions.lit(flinkLiteral)));
    org.apache.iceberg.expressions.Expression actual = FlinkFilters.convert(expr).get();
    assertPredicatesMatch(org.apache.iceberg.expressions.Expressions.equal(fieldName, icebergLiteral),
        (UnboundPredicate) actual);
  }

  private static Expression resolve(Expression originalExpression) {
    return originalExpression.accept(new ApiExpressionDefaultVisitor<Expression>() {
      @Override
      public Expression visit(UnresolvedReferenceExpression unresolvedReference) {
        String name = unresolvedReference.getName();
        TableColumn field = TABLE_SCHEMA.getTableColumn(name).get();
        int index = TABLE_SCHEMA.getTableColumns().indexOf(field);
        return new FieldReferenceExpression(name, field.getType(), 0, index);
      }

      @Override
      public Expression visit(UnresolvedCallExpression unresolvedCall) {
        List children = unresolvedCall.getChildren().stream().map(e -> e.accept(this)).collect(Collectors.toList());
        return new CallExpression(unresolvedCall.getFunctionDefinition(), children, DataTypes.STRING());
      }

      @Override
      public Expression visit(ValueLiteralExpression valueLiteral) {
        return valueLiteral;
      }

      @Override
      protected Expression defaultMethod(Expression expression) {
        throw new UnsupportedOperationException(String.format("unsupported expression: %s", expression));
      }
    });
  }

  private void assertPredicatesMatch(UnboundPredicate expected, UnboundPredicate actual) {
    assertEquals(expected.op(), actual.op());
    assertEquals(expected.literal(), actual.literal());
    assertEquals(expected.ref().name(), actual.ref().name());
  }
}
