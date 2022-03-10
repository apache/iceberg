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

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

public class TestExpressionParser {
  @Test
  public void testPredicate() {
    String expected = "{\n" +
            "  \"type\" : \"unbounded-predicate\",\n" +
            "  \"operation\" : \"in\",\n" +
            "  \"term\" : {\n" +
            "    \"type\" : \"named-reference\",\n" +
            "    \"value\" : \"Column-Name\"\n" +
            "  },\n" +
            "  \"literals\" : [ {\n" +
            "    \"type\" : \"int\",\n" +
            "    \"value\" : \"2\\u0000\\u0000\\u0000\"\n" +
            "  } ]\n" +
            "}";

    UnboundPredicate inPredicate = new UnboundPredicate(
            Expression.Operation.IN,
            new NamedReference("Column-Name"),
            Literal.of(50));

    String actual = ExpressionParser.toJson(inPredicate, true);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testAnd() {
    String expected = "{\n" +
            "  \"type\" : \"and\",\n" +
            "  \"left-operand\" : {\n" +
            "    \"type\" : \"unbounded-predicate\",\n" +
            "    \"operation\" : \"gt_eq\",\n" +
            "    \"term\" : {\n" +
            "      \"type\" : \"named-reference\",\n" +
            "      \"value\" : \"Column1-Name\"\n" +
            "    },\n" +
            "    \"literals\" : [ {\n" +
            "      \"type\" : \"int\",\n" +
            "      \"value\" : \"2\\u0000\\u0000\\u0000\"\n" +
            "    } ]\n" +
            "  },\n" +
            "  \"right-operand\" : {\n" +
            "    \"type\" : \"unbounded-predicate\",\n" +
            "    \"operation\" : \"in\",\n" +
            "    \"term\" : {\n" +
            "      \"type\" : \"named-reference\",\n" +
            "      \"value\" : \"Column2-Name\"\n" +
            "    },\n" +
            "    \"literals\" : [ {\n" +
            "      \"type\" : \"string\",\n" +
            "      \"value\" : \"Check\"\n" +
            "    } ]\n" +
            "  }\n" +
            "}";

    UnboundPredicate gtEqPredicate = new UnboundPredicate(
            Expression.Operation.GT_EQ,
            new NamedReference("Column1-Name"),
            Literal.of(50));

    UnboundPredicate inPredicate = new UnboundPredicate(
            Expression.Operation.IN,
            new NamedReference("Column2-Name"),
            Literal.of("Check"));

    And andExpression = new And(gtEqPredicate, inPredicate);

    Expression test = ExpressionParser.fromJson(expected);

    String actual = ExpressionParser.toJson(andExpression, true);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testOr() {
    String expected = "{\n" +
            "  \"type\" : \"or\",\n" +
            "  \"left-operand\" : {\n" +
            "    \"type\" : \"unbounded-predicate\",\n" +
            "    \"operation\" : \"lt\",\n" +
            "    \"term\" : {\n" +
            "      \"type\" : \"named-reference\",\n" +
            "      \"value\" : \"Column1-Name\"\n" +
            "    },\n" +
            "    \"literals\" : [ {\n" +
            "      \"type\" : \"int\",\n" +
            "      \"value\" : \"2\\u0000\\u0000\\u0000\"\n" +
            "    } ]\n" +
            "  },\n" +
            "  \"right-operand\" : {\n" +
            "    \"type\" : \"unbounded-predicate\",\n" +
            "    \"operation\" : \"not_null\",\n" +
            "    \"term\" : {\n" +
            "      \"type\" : \"named-reference\",\n" +
            "      \"value\" : \"Column2-Name\"\n" +
            "    }\n" +
            "  }\n" +
            "}";

    UnboundPredicate ltPredicate = new UnboundPredicate(
            Expression.Operation.LT,
            new NamedReference("Column1-Name"),
            Literal.of(50));

    UnboundPredicate notNullPredicate = new UnboundPredicate(
            Expression.Operation.NOT_NULL,
            new NamedReference("Column2-Name"));

    Or orExpression = new Or(ltPredicate, notNullPredicate);
    String actual = ExpressionParser.toJson(orExpression, true);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testNot() {
    String expected = "{\n" +
            "  \"type\" : \"not\",\n" +
            "  \"operand\" : {\n" +
            "    \"type\" : \"unbounded-predicate\",\n" +
            "    \"operation\" : \"lt\",\n" +
            "    \"term\" : {\n" +
            "      \"type\" : \"named-reference\",\n" +
            "      \"value\" : \"Column1-Name\"\n" +
            "    },\n" +
            "    \"literals\" : [ {\n" +
            "      \"type\" : \"int\",\n" +
            "      \"value\" : \"2\\u0000\\u0000\\u0000\"\n" +
            "    } ]\n" +
            "  }\n" +
            "}";

    UnboundPredicate ltPredicate = new UnboundPredicate(
            Expression.Operation.LT,
            new NamedReference("Column1-Name"),
            Literal.of(50));

    Not notExpression = new Not(ltPredicate);
    String actual = ExpressionParser.toJson(notExpression, true);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testNestedExpression() {
    String expected = "{\n" +
            "  \"type\" : \"or\",\n" +
            "  \"left-operand\" : {\n" +
            "    \"type\" : \"and\",\n" +
            "    \"left-operand\" : {\n" +
            "      \"type\" : \"unbounded-predicate\",\n" +
            "      \"operation\" : \"in\",\n" +
            "      \"term\" : {\n" +
            "        \"type\" : \"named-reference\",\n" +
            "        \"value\" : \"Column1-Name\"\n" +
            "      },\n" +
            "      \"literals\" : [ {\n" +
            "        \"type\" : \"int\",\n" +
            "        \"value\" : \"2\\u0000\\u0000\\u0000\"\n" +
            "      } ]\n" +
            "    },\n" +
            "    \"right-operand\" : {\n" +
            "      \"type\" : \"unbounded-predicate\",\n" +
            "      \"operation\" : \"eq\",\n" +
            "      \"term\" : {\n" +
            "        \"type\" : \"named-reference\",\n" +
            "        \"value\" : \"Column2-Name\"\n" +
            "      },\n" +
            "      \"literals\" : [ {\n" +
            "        \"type\" : \"string\",\n" +
            "        \"value\" : \"Test\"\n" +
            "      } ]\n" +
            "    }\n" +
            "  },\n" +
            "  \"right-operand\" : {\n" +
            "    \"type\" : \"unbounded-predicate\",\n" +
            "    \"operation\" : \"is_nan\",\n" +
            "    \"term\" : {\n" +
            "      \"type\" : \"named-reference\",\n" +
            "      \"value\" : \"Column3-Name\"\n" +
            "    }\n" +
            "  }\n" +
            "}";

    UnboundPredicate inPredicate = new UnboundPredicate(
            Expression.Operation.IN,
            new NamedReference("Column1-Name"),
            Literal.of(50));

    UnboundPredicate eqPredicate = new UnboundPredicate(
            Expression.Operation.EQ,
            new NamedReference("Column2-Name"),
            Literal.of("Test"));

    UnboundPredicate isNanPredicate = new UnboundPredicate(
            Expression.Operation.IS_NAN,
            new NamedReference("Column3-Name"));

    And andExpression = new And(inPredicate, eqPredicate);
    Or orNestedExpression = new Or(andExpression, isNanPredicate);
    String actual = ExpressionParser.toJson(orNestedExpression, true);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testParserBothWays() {
    String expected = "{\n" +
            "  \"type\" : \"or\",\n" +
            "  \"left-operand\" : {\n" +
            "    \"type\" : \"and\",\n" +
            "    \"left-operand\" : {\n" +
            "      \"type\" : \"unbounded-predicate\",\n" +
            "      \"operation\" : \"in\",\n" +
            "      \"term\" : {\n" +
            "        \"type\" : \"named-reference\",\n" +
            "        \"value\" : \"Column1-Name\"\n" +
            "      },\n" +
            "      \"literals\" : [ {\n" +
            "        \"type\" : \"int\",\n" +
            "        \"value\" : \"2\\u0000\\u0000\\u0000\"\n" +
            "      } ]\n" +
            "    },\n" +
            "    \"right-operand\" : {\n" +
            "      \"type\" : \"unbounded-predicate\",\n" +
            "      \"operation\" : \"eq\",\n" +
            "      \"term\" : {\n" +
            "        \"type\" : \"named-reference\",\n" +
            "        \"value\" : \"Column2-Name\"\n" +
            "      },\n" +
            "      \"literals\" : [ {\n" +
            "        \"type\" : \"string\",\n" +
            "        \"value\" : \"Test\"\n" +
            "      } ]\n" +
            "    }\n" +
            "  },\n" +
            "  \"right-operand\" : {\n" +
            "    \"type\" : \"unbounded-predicate\",\n" +
            "    \"operation\" : \"is_nan\",\n" +
            "    \"term\" : {\n" +
            "      \"type\" : \"named-reference\",\n" +
            "      \"value\" : \"Column3-Name\"\n" +
            "    }\n" +
            "  }\n" +
            "}";

    Expression actualExpression = ExpressionParser.fromJson(expected);
    String actualJsonExpression = ExpressionParser.toJson(actualExpression, true);

    Assert.assertEquals(expected, actualJsonExpression);
  }

  @Test
  public void testFixedLiteral() {
    String expected = "{\n" +
            "  \"type\" : \"unbounded-predicate\",\n" +
            "  \"operation\" : \"eq\",\n" +
            "  \"term\" : {\n" +
            "    \"type\" : \"named-reference\",\n" +
            "    \"value\" : \"Column-Name\"\n" +
            "  },\n" +
            "  \"literals\" : [ {\n" +
            "    \"type\" : \"fixed[10]\",\n" +
            "    \"value\" : \"testString\"\n" +
            "  } ]\n" +
            "}";
    String testString = "testString";
    ByteBuffer testByteBuffer = StandardCharsets.UTF_8.encode(testString);

    byte[] testByteArray = new byte[testByteBuffer.remaining()];
    testByteBuffer.get(testByteArray);

    UnboundPredicate expectedExpression = new UnboundPredicate(
            Expression.Operation.EQ,
            new NamedReference("Column-Name"),
            Lists.newArrayList(testByteArray));

    String actualJsonExpression = ExpressionParser.toJson(expectedExpression, true);
    Expression actualExpression = ExpressionParser.fromJson(actualJsonExpression);
    String newActualJsonExpression = ExpressionParser.toJson(actualExpression, true);

    Assert.assertEquals(expected, actualJsonExpression);
    Assert.assertEquals(actualJsonExpression, newActualJsonExpression);
  }

  @Test
  public void testDecimalLiteral() {
    String expected = "{\n" +
            "  \"type\" : \"unbounded-predicate\",\n" +
            "  \"operation\" : \"in\",\n" +
            "  \"term\" : {\n" +
            "    \"type\" : \"named-reference\",\n" +
            "    \"value\" : \"Column-Name\"\n" +
            "  },\n" +
            "  \"literals\" : [ {\n" +
            "    \"type\" : \"decimal(3, 2)\",\n" +
            "    \"value\" : \"\\u0001:\"\n" +
            "  } ]\n" +
            "}";

    UnboundPredicate expectedExpression = new UnboundPredicate(
            Expression.Operation.IN,
            new NamedReference("Column-Name"),
            Lists.newArrayList(new BigDecimal("3.14")));

    String actualJsonExpression = ExpressionParser.toJson(ExpressionParser.fromJson(expected), true);
    Expression actualExpression = ExpressionParser.fromJson(expected);

    Assert.assertEquals(expected, ExpressionParser.toJson(expectedExpression, true));
    Assert.assertEquals(expected, actualJsonExpression);
    Assert.assertEquals(
            ((UnboundPredicate) actualExpression).literals().get(0).toString(),
            expectedExpression.literals().get(0).toString());
  }
}
