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
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.expressions.ApiExpressionUtils;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.UnresolvedCallExpression;
import org.apache.flink.table.expressions.UnresolvedReferenceExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.expressions.utils.ApiExpressionDefaultVisitor;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.iceberg.expressions.And;
import org.apache.iceberg.expressions.BoundLiteralPredicate;
import org.apache.iceberg.expressions.Not;
import org.apache.iceberg.expressions.Or;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.util.DateTimeUtil;
import org.junit.Assert;
import org.junit.Test;

public class TestFlinkFilters {

  private static final TableSchema TABLE_SCHEMA = TableSchema.builder()
      .field("field1", DataTypes.INT())
      .field("field2", DataTypes.BIGINT())
      .field("field3", DataTypes.FLOAT())
      .field("field4", DataTypes.DOUBLE())
      .field("field5", DataTypes.STRING())
      .field("field6", DataTypes.BOOLEAN())
      .field("field7", DataTypes.BINARY(2))
      .field("field8", DataTypes.DECIMAL(10, 2))
      .field("field9", DataTypes.DATE())
      .field("field10", DataTypes.TIME())
      .field("field11", DataTypes.TIMESTAMP())
      .field("field12", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE())
      .build();

  // A map list of fields and values used to verify the conversion of flink expression to iceberg expression
  private static final List<Tuple2<String, Object>> FIELD_VALUE_LIST = ImmutableList.of(
      Tuple2.of("field1", 1),
      Tuple2.of("field2", 2L),
      Tuple2.of("field3", 3F),
      Tuple2.of("field4", 4D),
      Tuple2.of("field5", "iceberg"),
      Tuple2.of("field6", true),
      Tuple2.of("field7", new byte[] {'a', 'b'}),
      Tuple2.of("field8", BigDecimal.valueOf(10)),
      Tuple2.of("field9", DateTimeUtil.daysFromDate(LocalDate.now())),
      Tuple2.of("field10", DateTimeUtil.microsFromTime(LocalTime.now())),
      Tuple2.of("field11", DateTimeUtil.microsFromTimestamp(LocalDateTime.now())),
      Tuple2.of("field12", DateTimeUtil.microsFromInstant(Instant.now()))
  );

  @Test
  public void testFlinkDataTypeEqual() {
    matchLiteral("field1", 1, 1);
    matchLiteral("field2", 10L, 10L);
    matchLiteral("field3", 1.2F, 1.2F);
    matchLiteral("field4", 3.4D, 3.4D);
    matchLiteral("field5", "abcd", "abcd");
    matchLiteral("field6", true, true);
    matchLiteral("field7", new byte[] {'a', 'b'}, ByteBuffer.wrap(new byte[] {'a', 'b'}));
    matchLiteral("field8", BigDecimal.valueOf(10), BigDecimal.valueOf(10));

    LocalDate date = LocalDate.parse("2020-12-23");
    matchLiteral("field9", date, DateTimeUtil.daysFromDate(date));

    LocalTime time = LocalTime.parse("12:13:14");
    matchLiteral("field10", time, DateTimeUtil.microsFromTime(time));

    LocalDateTime dateTime = LocalDateTime.parse("2020-12-23T12:13:14");
    matchLiteral("field11", dateTime, DateTimeUtil.microsFromTimestamp(dateTime));

    Instant instant = Instant.parse("2020-12-23T12:13:14.00Z");
    matchLiteral("field12", instant, DateTimeUtil.microsFromInstant(instant));
  }

  @Test
  public void testEquals() {
    for (Tuple2<String, Object> tuple2 : FIELD_VALUE_LIST) {
      UnboundPredicate<?> expected = org.apache.iceberg.expressions.Expressions.equal(tuple2.f0, tuple2.f1);

      Optional<org.apache.iceberg.expressions.Expression> actual =
          FlinkFilters.convert(resolve(Expressions.$(tuple2.f0).isEqual(Expressions.lit(tuple2.f1))));
      Assert.assertTrue(actual.isPresent());
      assertPredicatesMatch(expected, (UnboundPredicate<?>) actual.get());

      Optional<org.apache.iceberg.expressions.Expression> actual1 =
          FlinkFilters.convert(resolve(Expressions.lit(tuple2.f1).isEqual(Expressions.$(tuple2.f0))));
      Assert.assertTrue(actual1.isPresent());
      assertPredicatesMatch(expected, (UnboundPredicate<?>) actual1.get());
    }
  }

  @Test
  public void testEqualsNaN() {
    UnboundPredicate<Float> expected = org.apache.iceberg.expressions.Expressions.isNaN("field3");

    Optional<org.apache.iceberg.expressions.Expression> actual =
        FlinkFilters.convert(resolve(Expressions.$("field3").isEqual(Expressions.lit(Float.NaN))));
    Assert.assertTrue(actual.isPresent());
    assertPredicatesMatch(expected, (UnboundPredicate<?>) actual.get());

    Optional<org.apache.iceberg.expressions.Expression> actual1 =
        FlinkFilters.convert(resolve(Expressions.lit(Float.NaN).isEqual(Expressions.$("field3"))));
    Assert.assertTrue(actual1.isPresent());
    assertPredicatesMatch(expected, (UnboundPredicate<?>) actual1.get());
  }

  @Test
  public void testNotEquals() {
    for (Tuple2<String, Object> tuple2 : FIELD_VALUE_LIST) {
      UnboundPredicate<?> expected = org.apache.iceberg.expressions.Expressions.notEqual(tuple2.f0, tuple2.f1);

      Optional<org.apache.iceberg.expressions.Expression> actual =
          FlinkFilters.convert(resolve(Expressions.$(tuple2.f0).isNotEqual(Expressions.lit(tuple2.f1))));
      Assert.assertTrue(actual.isPresent());
      assertPredicatesMatch(expected, (UnboundPredicate<?>) actual.get());

      Optional<org.apache.iceberg.expressions.Expression> actual1 =
          FlinkFilters.convert(resolve(Expressions.lit(tuple2.f1).isNotEqual(Expressions.$(tuple2.f0))));
      Assert.assertTrue(actual1.isPresent());
      assertPredicatesMatch(expected, (UnboundPredicate<?>) actual1.get());
    }
  }

  @Test
  public void testNotEqualsNaN() {
    UnboundPredicate<Float> expected = org.apache.iceberg.expressions.Expressions.notNaN("field3");

    Optional<org.apache.iceberg.expressions.Expression> actual =
        FlinkFilters.convert(resolve(Expressions.$("field3").isNotEqual(Expressions.lit(Float.NaN))));
    Assert.assertTrue(actual.isPresent());
    assertPredicatesMatch(expected, (UnboundPredicate<?>) actual.get());

    Optional<org.apache.iceberg.expressions.Expression> actual1 =
        FlinkFilters.convert(resolve(Expressions.lit(Float.NaN).isNotEqual(Expressions.$("field3"))));
    Assert.assertTrue(actual1.isPresent());
    assertPredicatesMatch(expected, (UnboundPredicate<?>) actual1.get());
  }

  @Test
  public void testGreaterThan() {
    UnboundPredicate<Integer> expected = org.apache.iceberg.expressions.Expressions.greaterThan("field1", 1);

    Optional<org.apache.iceberg.expressions.Expression> actual =
        FlinkFilters.convert(resolve(Expressions.$("field1").isGreater(Expressions.lit(1))));
    Assert.assertTrue(actual.isPresent());
    assertPredicatesMatch(expected, (UnboundPredicate<?>) actual.get());

    Optional<org.apache.iceberg.expressions.Expression> actual1 =
        FlinkFilters.convert(resolve(Expressions.lit(1).isLess(Expressions.$("field1"))));
    Assert.assertTrue(actual1.isPresent());
    assertPredicatesMatch(expected, (UnboundPredicate<?>) actual1.get());
  }

  @Test
  public void testGreaterThanEquals() {
    UnboundPredicate<Integer> expected = org.apache.iceberg.expressions.Expressions.greaterThanOrEqual("field1", 1);

    Optional<org.apache.iceberg.expressions.Expression> actual =
        FlinkFilters.convert(resolve(Expressions.$("field1").isGreaterOrEqual(Expressions.lit(1))));
    Assert.assertTrue(actual.isPresent());
    assertPredicatesMatch(expected, (UnboundPredicate<?>) actual.get());

    Optional<org.apache.iceberg.expressions.Expression> actual1 =
        FlinkFilters.convert(resolve(Expressions.lit(1).isLessOrEqual(Expressions.$("field1"))));
    Assert.assertTrue(actual1.isPresent());
    assertPredicatesMatch(expected, (UnboundPredicate<?>) actual1.get());
  }

  @Test
  public void testLessThan() {
    UnboundPredicate<Integer> expected = org.apache.iceberg.expressions.Expressions.lessThan("field1", 1);

    Optional<org.apache.iceberg.expressions.Expression> actual =
        FlinkFilters.convert(resolve(Expressions.$("field1").isLess(Expressions.lit(1))));
    Assert.assertTrue(actual.isPresent());
    assertPredicatesMatch(expected, (UnboundPredicate<?>) actual.get());

    Optional<org.apache.iceberg.expressions.Expression> actual1 =
        FlinkFilters.convert(resolve(Expressions.lit(1).isGreater(Expressions.$("field1"))));
    Assert.assertTrue(actual1.isPresent());
    assertPredicatesMatch(expected, (UnboundPredicate<?>) actual1.get());
  }

  @Test
  public void testLessThanEquals() {
    UnboundPredicate<Integer> expected = org.apache.iceberg.expressions.Expressions.lessThanOrEqual("field1", 1);

    Optional<org.apache.iceberg.expressions.Expression> actual =
        FlinkFilters.convert(resolve(Expressions.$("field1").isLessOrEqual(Expressions.lit(1))));
    Assert.assertTrue(actual.isPresent());
    assertPredicatesMatch(expected, (UnboundPredicate<?>) actual.get());

    Optional<org.apache.iceberg.expressions.Expression> actual1 =
        FlinkFilters.convert(resolve(Expressions.lit(1).isGreaterOrEqual(Expressions.$("field1"))));
    Assert.assertTrue(actual1.isPresent());
    assertPredicatesMatch(expected, (UnboundPredicate<?>) actual1.get());
  }

  @Test
  public void testIsNull() {
    Expression expr = resolve(Expressions.$("field1").isNull());
    Optional<org.apache.iceberg.expressions.Expression> actual = FlinkFilters.convert(expr);
    Assert.assertTrue(actual.isPresent());
    UnboundPredicate<Object> expected = org.apache.iceberg.expressions.Expressions.isNull("field1");
    assertPredicatesMatch(expected, (UnboundPredicate<?>) actual.get());
  }

  @Test
  public void testIsNotNull() {
    Expression expr = resolve(Expressions.$("field1").isNotNull());
    Optional<org.apache.iceberg.expressions.Expression> actual = FlinkFilters.convert(expr);
    Assert.assertTrue(actual.isPresent());
    UnboundPredicate<Object> expected = org.apache.iceberg.expressions.Expressions.notNull("field1");
    assertPredicatesMatch(expected, (UnboundPredicate<?>) actual.get());
  }

  @Test
  public void testAnd() {
    Expression expr = resolve(
        Expressions.$("field1").isEqual(Expressions.lit(1)).and(Expressions.$("field2").isEqual(Expressions.lit(2L))));
    Optional<org.apache.iceberg.expressions.Expression> actual = FlinkFilters.convert(expr);
    Assert.assertTrue(actual.isPresent());
    And and = (And) actual.get();
    And expected = (And) org.apache.iceberg.expressions.Expressions.and(
        org.apache.iceberg.expressions.Expressions.equal("field1", 1),
        org.apache.iceberg.expressions.Expressions.equal("field2", 2L));

    Assert.assertEquals(expected.op(), and.op());
    Assert.assertEquals(expected.left().op(), and.left().op());
    Assert.assertEquals(expected.right().op(), and.right().op());
  }

  @Test
  public void testOr() {
    Expression expr = resolve(
        Expressions.$("field1").isEqual(Expressions.lit(1)).or(Expressions.$("field2").isEqual(Expressions.lit(2L))));
    Optional<org.apache.iceberg.expressions.Expression> actual = FlinkFilters.convert(expr);
    Assert.assertTrue(actual.isPresent());
    Or or = (Or) actual.get();
    Or expected = (Or) org.apache.iceberg.expressions.Expressions.or(
        org.apache.iceberg.expressions.Expressions.equal("field1", 1),
        org.apache.iceberg.expressions.Expressions.equal("field2", 2L));

    Assert.assertEquals(expected.op(), or.op());
    Assert.assertEquals(expected.left().op(), or.left().op());
    Assert.assertEquals(expected.right().op(), or.right().op());
  }

  @Test
  public void testNot() {
    Expression expr = resolve(ApiExpressionUtils.unresolvedCall(
        BuiltInFunctionDefinitions.NOT, Expressions.$("field1").isEqual(Expressions.lit(1))));
    Optional<org.apache.iceberg.expressions.Expression> actual = FlinkFilters.convert(expr);
    Assert.assertTrue(actual.isPresent());
    Not not = (Not) actual.get();
    Not expected = (Not) org.apache.iceberg.expressions.Expressions.not(
        org.apache.iceberg.expressions.Expressions.equal("field1", 1));

    Assert.assertEquals(expected.op(), not.op());
    assertPredicatesMatch((UnboundPredicate<?>) expected.child(), (UnboundPredicate<?>) not.child());
  }

  @Test
  public void testLike() {
    UnboundPredicate<?> expected = org.apache.iceberg.expressions.Expressions.startsWith("field5", "abc");
    Expression expr = resolve(ApiExpressionUtils.unresolvedCall(
        BuiltInFunctionDefinitions.LIKE, Expressions.$("field5"), Expressions.lit("abc%")));
    Optional<org.apache.iceberg.expressions.Expression> actual = FlinkFilters.convert(expr);
    Assert.assertTrue(actual.isPresent());
    assertPredicatesMatch(expected, (UnboundPredicate<?>) actual.get());
  }

  private void matchLiteral(String fieldName, Object flinkLiteral, Object icebergLiteral) {
    Expression expr = resolve(Expressions.$(fieldName).isEqual(Expressions.lit(flinkLiteral)));
    Optional<org.apache.iceberg.expressions.Expression> actual = FlinkFilters.convert(expr);
    Assert.assertTrue(actual.isPresent());

    BoundLiteralPredicate predicate =
        (BoundLiteralPredicate<?>) ((UnboundPredicate<?>) actual.get())
            .bind(FlinkSchemaUtil.convert(TABLE_SCHEMA).asStruct(), false);
    Assert.assertTrue(predicate.test(icebergLiteral));
  }

  private static Expression resolve(Expression originalExpression) {
    return originalExpression.accept(new ApiExpressionDefaultVisitor<Expression>() {
      @Override
      public Expression visit(UnresolvedReferenceExpression unresolvedReference) {
        String name = unresolvedReference.getName();
        Optional<TableColumn> field = TABLE_SCHEMA.getTableColumn(name);
        if (field.isPresent()) {
          int index = TABLE_SCHEMA.getTableColumns().indexOf(field.get());
          return new FieldReferenceExpression(name, field.get().getType(), 0, index);
        } else {
          return null;
        }
      }

      @Override
      public Expression visit(UnresolvedCallExpression unresolvedCall) {
        List<ResolvedExpression> children =
            unresolvedCall.getChildren().stream().map(e -> (ResolvedExpression) e.accept(this))
                .collect(Collectors.toList());
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

  private void assertPredicatesMatch(UnboundPredicate<?> expected, UnboundPredicate<?> actual) {
    Assert.assertEquals(expected.op(), actual.op());
    Assert.assertEquals(expected.literal(), actual.literal());
    Assert.assertEquals(expected.ref().name(), actual.ref().name());
  }
}
