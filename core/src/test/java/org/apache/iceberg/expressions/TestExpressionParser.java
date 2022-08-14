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
package org.apache.iceberg.expressions;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import java.util.UUID;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

public class TestExpressionParser {

  private static final Types.StructType SUPPORTED_PRIMITIVES =
      Types.StructType.of(
          required(100, "id", Types.LongType.get()),
          optional(101, "data", Types.StringType.get()),
          required(102, "b", Types.BooleanType.get()),
          optional(103, "i", Types.IntegerType.get()),
          required(104, "l", Types.LongType.get()),
          optional(105, "f", Types.FloatType.get()),
          required(106, "d", Types.DoubleType.get()),
          optional(107, "date", Types.DateType.get()),
          required(108, "ts", Types.TimestampType.withZone()),
          required(110, "s", Types.StringType.get()),
          required(111, "uuid", Types.UUIDType.get()),
          required(112, "fixed", Types.FixedType.ofLength(7)),
          optional(113, "bytes", Types.BinaryType.get()),
          required(114, "dec_9_0", Types.DecimalType.of(9, 0)),
          required(115, "dec_11_2", Types.DecimalType.of(11, 2)),
          required(116, "dec_38_10", Types.DecimalType.of(38, 10)), // maximum precision
          required(117, "time", Types.TimeType.get()));
  private static final Schema SCHEMA = new Schema(SUPPORTED_PRIMITIVES.fields());

  @Test
  public void testExpressionParser() {
    Expression[] expressions = new Expression[] {
        Expressions.alwaysFalse(),
        Expressions.alwaysTrue(),
        Expressions.equal("id", 100),
        Expressions.and(
            Expressions.or(
                Expressions.equal("data", UUID.randomUUID().toString()),
                Expressions.isNull("data")),
            Expressions.greaterThanOrEqual("id", 66)),
        Expressions.or(
            Expressions.greaterThan(Expressions.day("ts"), "2022-08-14"),
            Expressions.equal("date", "2022-08-14")),
        Expressions.not(Expressions.in("l", 1, 2, 3, 4))
    };

    for (Expression expr : expressions) {
      Expression bound = Binder.bind(SUPPORTED_PRIMITIVES, expr);
      String boundJson = ExpressionParser.toJson(bound, true);
      System.err.println("Bound JSON: " + boundJson);
      String unboundJson = ExpressionParser.toJson(expr, true);
      Assert.assertEquals("Bound and unbound should produce identical json", boundJson, unboundJson);
      Expression parsed = ExpressionParser.fromJson(boundJson, SCHEMA);
      Assert.assertTrue(
          "Round-trip value should be equivalent", ExpressionUtil.equivalent(expr, parsed, SUPPORTED_PRIMITIVES, true));
    }
  }

//  @Test
//  public void nullExpression() {
//    Assertions.assertThatThrownBy(() -> ExpressionParser.toJson(null))
//        .isInstanceOf(IllegalArgumentException.class)
//        .hasMessage("Invalid expression: null");
//
//    Assertions.assertThatThrownBy(() -> ExpressionParser.fromJson((JsonNode) null))
//        .isInstanceOf(IllegalArgumentException.class)
//        .hasMessage("Cannot parse expression from null object");
//  }
//
//  @Test
//  public void unsupportedExpression() {
//    Assertions.assertThatThrownBy(() -> ExpressionParser.toJson((Expression) () -> null))
//        .isInstanceOf(IllegalArgumentException.class)
//        .hasMessageStartingWith("Unsupported expression:");
//
//    Expression expression =
//        Binder.bind(
//            new Schema(required(1, "x", LongType.get())).asStruct(), Expressions.equal("x", 23L));
//    Assertions.assertThatThrownBy(() -> ExpressionParser.toJson(expression))
//        .isInstanceOf(IllegalArgumentException.class)
//        .hasMessage("Unsupported expression: ref(id=1, accessor-type=long) == 23");
//  }
//
//  @Test
//  public void trueExpression() {
//    String expected = "{\n" + "  \"type\" : \"true\"\n" + "}";
//    Assertions.assertThat(ExpressionParser.toJson(Expressions.alwaysTrue(), true))
//        .isEqualTo(expected);
//    Assertions.assertThat(ExpressionParser.toJson(ExpressionParser.fromJson(expected), true))
//        .isEqualTo(expected);
//  }
//
//  @Test
//  public void falseExpression() {
//    String expected = "{\n" + "  \"type\" : \"false\"\n" + "}";
//    Assertions.assertThat(ExpressionParser.toJson(Expressions.alwaysFalse(), true))
//        .isEqualTo(expected);
//    Assertions.assertThat(ExpressionParser.toJson(ExpressionParser.fromJson(expected), true))
//        .isEqualTo(expected);
//  }
//
//  @Test
//  public void eqExpression() {
//    String expected =
//        "{\n"
//            + "  \"type\" : \"eq\",\n"
//            + "  \"term\" : \"name\",\n"
//            + "  \"literals\" : [ {\n"
//            + "    \"type\" : \"int\",\n"
//            + "    \"value\" : \"\\u0019\\u0000\\u0000\\u0000\"\n"
//            + "  } ]\n"
//            + "}";
//    Assertions.assertThat(ExpressionParser.toJson(Expressions.equal("name", 25), true))
//        .isEqualTo(expected);
//    Expression expression = ExpressionParser.fromJson(expected);
//    Assertions.assertThat(ExpressionParser.toJson(expression, true)).isEqualTo(expected);
//  }
//
//  @Test
//  public void transformsNotSupported() {
//    Assertions.assertThatThrownBy(
//            () ->
//                ExpressionParser.toJson(
//                    Expressions.in(Expressions.transform("x", Transforms.day(DateType.get())), 25)))
//        .isInstanceOf(IllegalArgumentException.class)
//        .hasMessage("Unsupported term: day(ref(name=\"x\"))");
//  }
//
//  @Test
//  public void extraFields() {
//    Assertions.assertThat(
//            ExpressionParser.toJson(
//                ExpressionParser.fromJson(
//                    "{\n"
//                        + "  \"type\" : \"in\",\n"
//                        + "  \"term\" : \"column-name\",\n"
//                        + "  \"extra-one\" : \"x\",\n"
//                        + "  \"extra-twp\" : \"y\",\n"
//                        + "  \"literals\" : [ {\n"
//                        + "    \"type\" : \"int\",\n"
//                        + "    \"value\" : \"2\\u0000\\u0000\\u0000\"\n"
//                        + "  } ]\n"
//                        + "}"),
//                true))
//        .isEqualTo(
//            "{\n"
//                + "  \"type\" : \"in\",\n"
//                + "  \"term\" : \"column-name\",\n"
//                + "  \"literals\" : [ {\n"
//                + "    \"type\" : \"int\",\n"
//                + "    \"value\" : \"2\\u0000\\u0000\\u0000\"\n"
//                + "  } ]\n"
//                + "}");
//  }
//
//  @Test
//  public void invalidTerm() {
//    Assertions.assertThatThrownBy(
//            () ->
//                ExpressionParser.fromJson(
//                    "{\n"
//                        + "  \"type\" : \"not\",\n"
//                        + "  \"operand\" : {\n"
//                        + "    \"type\" : \"lt\",\n"
//                        + "    \"term\" : 23,\n"
//                        + "    \"literals\" : [ {\n"
//                        + "      \"type\" : \"int\",\n"
//                        + "      \"value\" : \"2\\u0000\\u0000\\u0000\"\n"
//                        + "    } ]\n"
//                        + "  }\n"
//                        + "}"))
//        .isInstanceOf(IllegalArgumentException.class)
//        .hasMessage("Cannot parse term to a string value: 23");
//  }
//
//  @Test
//  public void invalidOperationType() {
//    Assertions.assertThatThrownBy(
//            () ->
//                ExpressionParser.fromJson(
//                    "{\n"
//                        + "  \"type\" : \"not\",\n"
//                        + "  \"operand\" : {\n"
//                        + "    \"type\" : \"illegal\",\n"
//                        + "    \"term\" : \"column-name\",\n"
//                        + "    \"literals\" : [ {\n"
//                        + "      \"type\" : \"int\",\n"
//                        + "      \"value\" : \"2\\u0000\\u0000\\u0000\"\n"
//                        + "    } ]\n"
//                        + "  }\n"
//                        + "}"))
//        .isInstanceOf(IllegalArgumentException.class)
//        .hasMessage("No enum constant org.apache.iceberg.expressions.Expression.Operation.ILLEGAL");
//
//    Assertions.assertThatThrownBy(
//            () ->
//                ExpressionParser.fromJson(
//                    "{\n"
//                        + "  \"type\" : \"ILLEGAL\",\n"
//                        + "  \"operand\" : {\n"
//                        + "    \"type\" : \"lt\",\n"
//                        + "    \"term\" : \"column-name\",\n"
//                        + "    \"literals\" : [ {\n"
//                        + "      \"type\" : \"int\",\n"
//                        + "      \"value\" : \"2\\u0000\\u0000\\u0000\"\n"
//                        + "    } ]\n"
//                        + "  }\n"
//                        + "}"))
//        .isInstanceOf(IllegalArgumentException.class)
//        .hasMessage("No enum constant org.apache.iceberg.expressions.Expression.Operation.ILLEGAL");
//  }
//
//  @Test
//  public void invalidAnd() {
//    Assertions.assertThatThrownBy(
//            () -> ExpressionParser.fromJson("{\n" + "  \"type\" : \"and\"\n" + "}"))
//        .isInstanceOf(IllegalArgumentException.class)
//        .hasMessage("Cannot parse missing field: left");
//
//    Assertions.assertThatThrownBy(
//            () -> ExpressionParser.fromJson("{\n" + "  \"type\" : \"and\"\n" + "}"))
//        .isInstanceOf(IllegalArgumentException.class)
//        .hasMessage("Cannot parse missing field: left");
//
//    Assertions.assertThatThrownBy(
//            () ->
//                ExpressionParser.fromJson(
//                    "{\n"
//                        + "  \"type\" : \"and\",\n"
//                        + "  \"left\" : {\n"
//                        + "    \"type\" : \"gt_eq\",\n"
//                        + "    \"term\" : \"column-name-1\",\n"
//                        + "    \"literals\" : [ {\n"
//                        + "      \"type\" : \"int\",\n"
//                        + "      \"value\" : \"2\\u0000\\u0000\\u0000\"\n"
//                        + "    } ]\n"
//                        + "  }\n"
//                        + "}"))
//        .isInstanceOf(IllegalArgumentException.class)
//        .hasMessage("Cannot parse missing field: right");
//
//    // term is missing from left
//    Assertions.assertThatThrownBy(
//            () ->
//                ExpressionParser.fromJson(
//                    "{\n"
//                        + "  \"type\" : \"and\",\n"
//                        + "  \"left\" : {\n"
//                        + "    \"type\" : \"gt_eq\",\n"
//                        + "    \"literals\" : [ {\n"
//                        + "      \"type\" : \"int\",\n"
//                        + "      \"value\" : \"2\\u0000\\u0000\\u0000\"\n"
//                        + "    } ]\n"
//                        + "  },\n"
//                        + "  \"right\" : {\n"
//                        + "    \"type\" : \"in\",\n"
//                        + "    \"term\" : \"column-name-2\",\n"
//                        + "    \"literals\" : [ {\n"
//                        + "      \"type\" : \"string\",\n"
//                        + "      \"value\" : \"Check\"\n"
//                        + "    } ]\n"
//                        + "  }\n"
//                        + "}"))
//        .isInstanceOf(IllegalArgumentException.class)
//        .hasMessage("Cannot parse missing string term");
//  }
//
//  @Test
//  public void testPredicate() {
//    String expected =
//        "{\n"
//            + "  \"type\" : \"in\",\n"
//            + "  \"term\" : \"column-name\",\n"
//            + "  \"literals\" : [ {\n"
//            + "    \"type\" : \"int\",\n"
//            + "    \"value\" : \"2\\u0000\\u0000\\u0000\"\n"
//            + "  } ]\n"
//            + "}";
//
//    Assertions.assertThat(ExpressionParser.toJson(Expressions.in("column-name", 50), true))
//        .isEqualTo(expected);
//    Assertions.assertThat(ExpressionParser.toJson(ExpressionParser.fromJson(expected), true))
//        .isEqualTo(expected);
//  }
//
//  @Test
//  public void testAnd() {
//    String expected =
//        "{\n"
//            + "  \"type\" : \"and\",\n"
//            + "  \"left\" : {\n"
//            + "    \"type\" : \"gt_eq\",\n"
//            + "    \"term\" : \"column-name-1\",\n"
//            + "    \"literals\" : [ {\n"
//            + "      \"type\" : \"int\",\n"
//            + "      \"value\" : \"2\\u0000\\u0000\\u0000\"\n"
//            + "    } ]\n"
//            + "  },\n"
//            + "  \"right\" : {\n"
//            + "    \"type\" : \"in\",\n"
//            + "    \"term\" : \"column-name-2\",\n"
//            + "    \"literals\" : [ {\n"
//            + "      \"type\" : \"string\",\n"
//            + "      \"value\" : \"Check\"\n"
//            + "    } ]\n"
//            + "  }\n"
//            + "}";
//
//    Expression expression =
//        Expressions.and(
//            Expressions.greaterThanOrEqual("column-name-1", 50),
//            Expressions.in("column-name-2", "Check"));
//
//    Assertions.assertThat(ExpressionParser.toJson(expression, true)).isEqualTo(expected);
//    Assertions.assertThat(ExpressionParser.toJson(ExpressionParser.fromJson(expected), true))
//        .isEqualTo(expected);
//  }
//
//  @Test
//  public void testOr() {
//    String expected =
//        "{\n"
//            + "  \"type\" : \"or\",\n"
//            + "  \"left\" : {\n"
//            + "    \"type\" : \"lt\",\n"
//            + "    \"term\" : \"column-name-1\",\n"
//            + "    \"literals\" : [ {\n"
//            + "      \"type\" : \"int\",\n"
//            + "      \"value\" : \"2\\u0000\\u0000\\u0000\"\n"
//            + "    } ]\n"
//            + "  },\n"
//            + "  \"right\" : {\n"
//            + "    \"type\" : \"not_null\",\n"
//            + "    \"term\" : \"column-name-2\"\n"
//            + "  }\n"
//            + "}";
//
//    Expression expression =
//        Expressions.or(
//            Expressions.lessThan("column-name-1", 50), Expressions.notNull("column-name-2"));
//    Assertions.assertThat(ExpressionParser.toJson(expression, true)).isEqualTo(expected);
//    Assertions.assertThat(ExpressionParser.toJson(ExpressionParser.fromJson(expected), true))
//        .isEqualTo(expected);
//  }
//
//  @Test
//  public void testNot() {
//    String expected =
//        "{\n"
//            + "  \"type\" : \"not\",\n"
//            + "  \"operand\" : {\n"
//            + "    \"type\" : \"lt\",\n"
//            + "    \"term\" : \"column-name-1\",\n"
//            + "    \"literals\" : [ {\n"
//            + "      \"type\" : \"int\",\n"
//            + "      \"value\" : \"2\\u0000\\u0000\\u0000\"\n"
//            + "    } ]\n"
//            + "  }\n"
//            + "}";
//
//    Expression expression = Expressions.not(Expressions.lessThan("column-name-1", 50));
//
//    Assertions.assertThat(ExpressionParser.toJson(expression, true)).isEqualTo(expected);
//    Assertions.assertThat(ExpressionParser.toJson(ExpressionParser.fromJson(expected), true))
//        .isEqualTo(expected);
//  }
//
//  @Test
//  public void testNestedExpression() {
//    String expected =
//        "{\n"
//            + "  \"type\" : \"or\",\n"
//            + "  \"left\" : {\n"
//            + "    \"type\" : \"and\",\n"
//            + "    \"left\" : {\n"
//            + "      \"type\" : \"in\",\n"
//            + "      \"term\" : \"column-name-1\",\n"
//            + "      \"literals\" : [ {\n"
//            + "        \"type\" : \"int\",\n"
//            + "        \"value\" : \"2\\u0000\\u0000\\u0000\"\n"
//            + "      } ]\n"
//            + "    },\n"
//            + "    \"right\" : {\n"
//            + "      \"type\" : \"eq\",\n"
//            + "      \"term\" : \"column-name-2\",\n"
//            + "      \"literals\" : [ {\n"
//            + "        \"type\" : \"string\",\n"
//            + "        \"value\" : \"Test\"\n"
//            + "      } ]\n"
//            + "    }\n"
//            + "  },\n"
//            + "  \"right\" : {\n"
//            + "    \"type\" : \"is_nan\",\n"
//            + "    \"term\" : \"column-name-3\"\n"
//            + "  }\n"
//            + "}";
//
//    Expression and =
//        Expressions.and(
//            Expressions.in("column-name-1", 50), Expressions.equal("column-name-2", "Test"));
//    Expression expression = Expressions.or(and, Expressions.isNaN("column-name-3"));
//
//    Assertions.assertThat(ExpressionParser.toJson(expression, true)).isEqualTo(expected);
//    Assertions.assertThat(ExpressionParser.toJson(ExpressionParser.fromJson(expected), true))
//        .isEqualTo(expected);
//  }
//
//  @Test
//  public void testFixedLiteral() {
//    String expected =
//        "{\n"
//            + "  \"type\" : \"eq\",\n"
//            + "  \"term\" : \"column-name\",\n"
//            + "  \"literals\" : [ {\n"
//            + "    \"type\" : \"fixed[10]\",\n"
//            + "    \"value\" : \"testString\"\n"
//            + "  } ]\n"
//            + "}";
//    String testString = "testString";
//    ByteBuffer byteBuffer = StandardCharsets.UTF_8.encode(testString);
//
//    byte[] byteArray = new byte[byteBuffer.remaining()];
//    byteBuffer.get(byteArray);
//
//    Expression expression = Expressions.equal("column-name", byteArray);
//
//    Assertions.assertThat(ExpressionParser.toJson(expression, true)).isEqualTo(expected);
//    Assertions.assertThat(ExpressionParser.toJson(ExpressionParser.fromJson(expected), true))
//        .isEqualTo(expected);
//  }
//
//  @Test
//  public void testDecimalLiteral() {
//    String expected =
//        "{\n"
//            + "  \"type\" : \"in\",\n"
//            + "  \"term\" : \"column-name\",\n"
//            + "  \"literals\" : [ {\n"
//            + "    \"type\" : \"decimal(3, 2)\",\n"
//            + "    \"value\" : \"\\u0001:\"\n"
//            + "  } ]\n"
//            + "}";
//
//    Expression expression = Expressions.in("column-name", new BigDecimal("3.14"));
//
//    Assertions.assertThat(ExpressionParser.toJson(expression, true)).isEqualTo(expected);
//    Assertions.assertThat(ExpressionParser.toJson(ExpressionParser.fromJson(expected), true))
//        .isEqualTo(expected);
//  }
}
