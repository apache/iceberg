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

import com.fasterxml.jackson.core.JsonGenerator;
import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.util.JsonUtil;

public class ExpressionParser {

  private static final String TYPE = "type";
  private static final String VALUE = "value";
  private static final String OPERATION = "operation";
  private static final String LITERALS = "literals";
  private static final String TERM = "term";
  private static final String LEFT_OPERAND = "left-operand";
  private static final String RIGHT_OPERAND = "right-operand";
  private static final String OPERAND = "operand";
  private static final String AND = "and";
  private static final String OR = "or";
  private static final String NOT = "not";
  private static final String TRUE = "true";
  private static final String FALSE = "false";
  private static final String UNBOUNDED_PREDICATE = "unbounded-predicate";
  private static final String BOUNDED_LITERAL_PREDICATE = "bounded-literal-predicate";
  private static final String BOUNDED_SET_PREDICATE = "bounded-set-predicate";
  private static final String BOUNDED_UNARY_PREDICATE = "bounded-unary-predicate";
  private static final String NAMED_REFERENCE = "named-reference";
  private static final String BOUND_REFERENCE = "bound-reference";
  private static final String ABOVE_MAX = "above-max";
  private static final String BELOW_MIN = "below-min";

  private static final Set<Expression.Operation> oneInputs = ImmutableSet.of(
          Expression.Operation.IS_NULL,
          Expression.Operation.NOT_NULL,
          Expression.Operation.IS_NAN,
          Expression.Operation.NOT_NAN);


  private ExpressionParser() {
  }

  public static String toJson(Expression expression, boolean pretty) {
    try {
      StringWriter writer = new StringWriter();
      JsonGenerator generator = JsonUtil.factory().createGenerator(writer);
      if (pretty) {
        generator.useDefaultPrettyPrinter();
      }
      toJson(expression, generator);
      generator.flush();
      return writer.toString();

    } catch (IOException e) {
      throw new UncheckedIOException("Failed to write json", e);
    }
  }

  public static void toJson(Expression expression, JsonGenerator generator) throws IOException {
    if (expression instanceof And) {
      toJson((And) expression, generator);
    } else if (expression instanceof Or) {
      toJson((Or) expression, generator);
    } else if (expression instanceof Not) {
      toJson((Not) expression, generator);
    } else if (expression instanceof True) {
      toJson((True) expression, generator);
    } else if (expression instanceof False) {
      toJson((False) expression, generator);
    } else if (expression instanceof Predicate) {
      toJson((Predicate) expression, generator);
    } else {
      throw new IllegalArgumentException("Invalid Operation Type");
    }
  }

  public static void toJson(And expression, JsonGenerator generator) throws IOException {
    generator.writeStartObject();
    generator.writeStringField(TYPE, AND);
    generator.writeFieldName(LEFT_OPERAND);
    toJson(expression.left(), generator);
    generator.writeFieldName(RIGHT_OPERAND);
    toJson(expression.right(), generator);
    generator.writeEndObject();
  }

  public static void toJson(Or expression, JsonGenerator generator) throws IOException {
    generator.writeStartObject();
    generator.writeStringField(TYPE, OR);
    generator.writeFieldName(LEFT_OPERAND);
    toJson(expression.left(), generator);
    generator.writeFieldName(RIGHT_OPERAND);
    toJson(expression.right(), generator);
    generator.writeEndObject();
  }

  public static void toJson(Not expression, JsonGenerator generator) throws IOException {
    generator.writeStartObject();
    generator.writeStringField(TYPE, NOT);
    generator.writeFieldName(OPERAND);
    toJson(expression.child(), generator);
    generator.writeEndObject();
  }

  public static void toJson(True expression, JsonGenerator generator) throws IOException {
    generator.writeStartObject();
    generator.writeStringField(TYPE, TRUE);
    generator.writeEndObject();
  }

  public static void toJson(False expression, JsonGenerator generator) throws IOException {
    generator.writeStartObject();
    generator.writeStringField(TYPE, FALSE);
    generator.writeEndObject();
  }

  public static void toJson(Predicate predicate, JsonGenerator generator) throws IOException {
    if (predicate instanceof UnboundPredicate) {
      toJson((UnboundPredicate) predicate, generator);
    } else if (predicate instanceof BoundLiteralPredicate) {
      toJson((BoundLiteralPredicate) predicate, generator);
    } else if (predicate instanceof BoundSetPredicate) {
      toJson((BoundSetPredicate) predicate, generator);
    } else if (predicate instanceof BoundUnaryPredicate) {
      toJson((BoundUnaryPredicate) predicate, generator);
    } else {
      throw new IllegalArgumentException("Cannot find valid Predicate Type for " + predicate + ".");
    }
  }

  public static void toJson(UnboundPredicate predicate, JsonGenerator generator) throws IOException {
    generator.writeStartObject();
    generator.writeStringField(TYPE, UNBOUNDED_PREDICATE);
    generator.writeStringField(OPERATION, predicate.op().name().toLowerCase());
    generator.writeFieldName(TERM);
    toJson(predicate.term(), generator);
    if (!(oneInputs.contains(predicate.op()))) {
      generator.writeFieldName(LITERALS);
      toJson(predicate.literals(), generator);
    }

    generator.writeEndObject();
  }

  public static void toJson(BoundLiteralPredicate predicate, JsonGenerator generator) {
    throw new UnsupportedOperationException(
            "Serialization of Predicate type BoundLiteralPredicate is not currently supported.");
  }

  public static void toJson(BoundSetPredicate predicate, JsonGenerator generator) {
    throw new UnsupportedOperationException(
            "Serialization of Predicate type BoundSetPredicate is not currently supported.");
  }

  public static void toJson(BoundUnaryPredicate predicate, JsonGenerator generator) {
    throw new UnsupportedOperationException(
            "Serialization of Predicate type BoundUnaryPredicate is not currently supported.");
  }

  public static void toJson(Term term, JsonGenerator generator) throws IOException {
    if (term instanceof NamedReference) {
      toJson((NamedReference) term, generator);
    } else if (term instanceof BoundReference) {
      toJson((BoundReference) term, generator); // Need to Implement
    } else {
      throw new IllegalArgumentException("Cannot find valid Term Type for " + term + ".");
    }
  }

  public static void toJson(NamedReference term, JsonGenerator generator) throws IOException {
    generator.writeStartObject();
    generator.writeStringField(TYPE, NAMED_REFERENCE);
    generator.writeStringField(VALUE, term.name());
    generator.writeEndObject();
  }

  public static void toJson(BoundReference term, JsonGenerator generator) {
    throw new UnsupportedOperationException("Serialization of Term type BoundReference is not currently supported");
  }

  public static void toJson(List<Literal> literals, JsonGenerator generator) throws IOException {
    generator.writeStartArray();
    for (int i = 0; i < literals.size(); i++) {
      toJson(literals.get(i), generator);
    }
    generator.writeEndArray();
  }

  public static void toJson(Literal literal, JsonGenerator generator) throws IOException {
    generator.writeStartObject();
    if (literal instanceof Literals.AboveMax) {
      generator.writeStringField(TYPE, ABOVE_MAX);
    } else if (literal instanceof Literals.BelowMin) {
      generator.writeStringField(TYPE, BELOW_MIN);
    } else {
      generator.writeStringField(TYPE, ((Literals.BaseLiteral) literal).typeId().toString().toLowerCase());
      generator.writeStringField(VALUE, StandardCharsets.UTF_8.decode(literal.toByteBuffer()).toString());
    }

    generator.writeEndObject();
  }
}
