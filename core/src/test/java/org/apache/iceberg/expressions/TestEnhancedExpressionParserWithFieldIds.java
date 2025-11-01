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
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.Supplier;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.JsonUtil;
import org.junit.jupiter.api.Test;

/**
 * Test demonstrating how ExpressionParser could be enhanced to support field IDs for
 * ResolvedReference serialization/deserialization.
 *
 * <p>This is a proof of concept showing the enhanced JSON format that would preserve field ID
 * information during serialization round-trips.
 */
public class TestEnhancedExpressionParserWithFieldIds {

  private static final Types.StructType STRUCT_TYPE =
      Types.StructType.of(
          required(100, "id", Types.LongType.get()),
          optional(101, "data", Types.StringType.get()),
          required(102, "active", Types.BooleanType.get()));

  private static final Schema SCHEMA = new Schema(STRUCT_TYPE.fields());

  // Enhanced JSON format constants for field ID support
  private static final String FIELD_ID = "field-id";
  private static final String REFERENCE_WITH_ID = "id-reference";

  @Test
  public void testEnhancedJsonFormatWithFieldIds() {
    // Test the enhanced JSON format that would support field IDs
    Expression resolvedExpr = Expressions.equal(Expressions.ref("data", 101), "test");

    // Generate enhanced JSON manually to show the concept
    String enhancedJson = generateEnhancedJson(resolvedExpr);

    // Expected enhanced JSON structure with field ID
    String expectedJson =
        "{\n"
            + "  \"type\" : \"eq\",\n"
            + "  \"term\" : {\n"
            + "    \"type\" : \"id-reference\",\n"
            + "    \"name\" : \"data\",\n"
            + "    \"field-id\" : 101\n"
            + "  },\n"
            + "  \"value\" : \"test\"\n"
            + "}";

    assertThat(enhancedJson).isEqualTo(expectedJson);
  }

  @Test
  public void testEnhancedParsingWithFieldIds() {
    // Test parsing enhanced JSON that includes field IDs
    String enhancedJson =
        "{\n"
            + "  \"type\" : \"eq\",\n"
            + "  \"term\" : {\n"
            + "    \"type\" : \"id-reference\",\n"
            + "    \"name\" : \"data\",\n"
            + "    \"field-id\" : 101\n"
            + "  },\n"
            + "  \"value\" : \"test\"\n"
            + "}";

    // Parse using enhanced parser (concept)
    Expression parsed = parseEnhancedJson(enhancedJson);

    assertThat(parsed).isInstanceOf(UnboundPredicate.class);
    UnboundPredicate<?> predicate = (UnboundPredicate<?>) parsed;
    assertThat(predicate.term()).isInstanceOf(IDReference.class);

    IDReference<?> resolvedRef = (IDReference<?>) predicate.term();
    assertThat(resolvedRef.name()).isEqualTo("data");
    assertThat(resolvedRef.id()).isEqualTo(101);
  }

  @Test
  public void testBackwardCompatibilityWithExistingFormat() {
    // Test that enhanced parser can handle existing JSON format without field IDs
    String standardJson =
        "{\n"
            + "  \"type\" : \"eq\",\n"
            + "  \"term\" : \"data\",\n"
            + "  \"value\" : \"test\"\n"
            + "}";

    Expression parsed = parseEnhancedJson(standardJson);

    assertThat(parsed).isInstanceOf(UnboundPredicate.class);
    UnboundPredicate<?> predicate = (UnboundPredicate<?>) parsed;
    assertThat(predicate.term()).isInstanceOf(NamedReference.class);

    NamedReference<?> namedRef = (NamedReference<?>) predicate.term();
    assertThat(namedRef.name()).isEqualTo("data");
  }

  @Test
  public void testComplexExpressionWithMixedReferences() {
    // Test complex expression with both ResolvedReference and NamedReference
    Expression mixedExpr =
        Expressions.and(
            Expressions.equal(Expressions.ref("data", 101), "test"), // ResolvedReference
            Expressions.isNull("active")); // NamedReference

    String enhancedJson = generateEnhancedJson(mixedExpr);

    // Should contain both reference types in JSON
    assertThat(enhancedJson).contains("\"id-reference\"");
    assertThat(enhancedJson).contains("\"field-id\" : 101");
    assertThat(enhancedJson).contains("\"active\"");

    // Parse back and verify
    Expression parsed = parseEnhancedJson(enhancedJson);
    assertThat(parsed).isInstanceOf(And.class);

    And andExpr = (And) parsed;

    // Left side should be ResolvedReference
    UnboundPredicate<?> leftPred = (UnboundPredicate<?>) andExpr.left();
    assertThat(leftPred.term()).isInstanceOf(IDReference.class);

    // Right side should be NamedReference
    UnboundPredicate<?> rightPred = (UnboundPredicate<?>) andExpr.right();
    assertThat(rightPred.term()).isInstanceOf(NamedReference.class);
  }

  @Test
  public void testFieldIdPreservationThroughRoundTrip() {
    // Test that field IDs are preserved through complete round-trip
    Expression original =
        Expressions.and(
            Expressions.greaterThan(Expressions.ref("id", 100), 50L),
            Expressions.equal(Expressions.ref("data", 101), "test"));

    // Generate enhanced JSON
    String json = generateEnhancedJson(original);

    // Parse back
    Expression parsed = parseEnhancedJson(json);

    // Verify structure is preserved
    assertThat(parsed).isInstanceOf(And.class);
    And andExpr = (And) parsed;

    // Check left predicate (id > 50)
    UnboundPredicate<?> leftPred = (UnboundPredicate<?>) andExpr.left();
    IDReference<?> leftRef = (IDReference<?>) leftPred.term();
    assertThat(leftRef.name()).isEqualTo("id");
    assertThat(leftRef.id()).isEqualTo(100);

    // Check right predicate (data = "test")
    UnboundPredicate<?> rightPred = (UnboundPredicate<?>) andExpr.right();
    IDReference<?> rightRef = (IDReference<?>) rightPred.term();
    assertThat(rightRef.name()).isEqualTo("data");
    assertThat(rightRef.id()).isEqualTo(101);
  }

  // Helper methods to demonstrate enhanced JSON generation and parsing

  private String generateEnhancedJson(Expression expr) {
    return JsonUtil.generate(
        gen -> {
          try {
            generateEnhanced(expr, gen);
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        },
        true);
  }

  private void generateEnhanced(Expression expr, JsonGenerator gen) throws IOException {
    ExpressionVisitors.visit(expr, new EnhancedJsonVisitor(gen));
  }

  private Expression parseEnhancedJson(String json) {
    try {
      JsonNode node = JsonUtil.mapper().readTree(json);
      return parseEnhancedExpression(node);
    } catch (Exception e) {
      throw new RuntimeException("Failed to parse JSON", e);
    }
  }

  // Enhanced JSON visitor that supports field IDs
  private static class EnhancedJsonVisitor
      extends ExpressionVisitors.CustomOrderExpressionVisitor<Void> {
    private final JsonGenerator gen;

    EnhancedJsonVisitor(JsonGenerator gen) {
      this.gen = gen;
    }

    private void toJson(Supplier<Void> child) {
      child.get();
    }

    private Void generate(Runnable task) {
      try {
        task.run();
        return null;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public Void alwaysTrue() {
      return generate(
          () -> {
            try {
              gen.writeBoolean(true);
            } catch (IOException e) {
              throw new UncheckedIOException(e);
            }
          });
    }

    @Override
    public Void alwaysFalse() {
      return generate(
          () -> {
            try {
              gen.writeBoolean(false);
            } catch (IOException e) {
              throw new UncheckedIOException(e);
            }
          });
    }

    @Override
    public Void not(Supplier<Void> result) {
      return generate(
          () -> {
            try {
              gen.writeStartObject();
              gen.writeStringField("type", "not");
              gen.writeFieldName("child");
              toJson(result);
              gen.writeEndObject();
            } catch (IOException e) {
              throw new UncheckedIOException(e);
            }
          });
    }

    @Override
    public Void and(Supplier<Void> leftResult, Supplier<Void> rightResult) {
      return generate(
          () -> {
            try {
              gen.writeStartObject();
              gen.writeStringField("type", "and");
              gen.writeFieldName("left");
              toJson(leftResult);
              gen.writeFieldName("right");
              toJson(rightResult);
              gen.writeEndObject();
            } catch (IOException e) {
              throw new UncheckedIOException(e);
            }
          });
    }

    @Override
    public Void or(Supplier<Void> leftResult, Supplier<Void> rightResult) {
      return generate(
          () -> {
            try {
              gen.writeStartObject();
              gen.writeStringField("type", "or");
              gen.writeFieldName("left");
              toJson(leftResult);
              gen.writeFieldName("right");
              toJson(rightResult);
              gen.writeEndObject();
            } catch (IOException e) {
              throw new UncheckedIOException(e);
            }
          });
    }

    @Override
    public <T> Void predicate(UnboundPredicate<T> pred) {
      return generate(
          () -> {
            try {
              gen.writeStartObject();
              gen.writeStringField("type", pred.op().toString().toLowerCase().replace("_", "-"));
              gen.writeFieldName("term");
              writeTerm(pred.term());

              if (pred.literals() != null && !pred.literals().isEmpty()) {
                if (pred.literals().size() == 1) {
                  gen.writeFieldName("value");
                  writeLiteral(pred.literals().get(0));
                } else {
                  gen.writeFieldName("values");
                  gen.writeStartArray();
                  for (Literal<T> literal : pred.literals()) {
                    writeLiteral(literal);
                  }
                  gen.writeEndArray();
                }
              }

              gen.writeEndObject();
            } catch (IOException e) {
              throw new UncheckedIOException(e);
            }
          });
    }

    private void writeTerm(UnboundTerm<?> term) throws IOException {
      if (term instanceof IDReference) {
        IDReference<?> resolvedRef = (IDReference<?>) term;
        gen.writeStartObject();
        gen.writeStringField("type", REFERENCE_WITH_ID);
        gen.writeStringField("name", resolvedRef.name());
        gen.writeNumberField(FIELD_ID, resolvedRef.id());
        gen.writeEndObject();
      } else if (term instanceof NamedReference) {
        gen.writeString(((NamedReference<?>) term).name());
      } else {
        throw new UnsupportedOperationException("Unsupported term type: " + term.getClass());
      }
    }

    private void writeLiteral(Literal<?> literal) throws IOException {
      Object value = literal.value();
      if (value instanceof String) {
        gen.writeString((String) value);
      } else if (value instanceof Long) {
        gen.writeNumber((Long) value);
      } else if (value instanceof Integer) {
        gen.writeNumber((Integer) value);
      } else if (value instanceof Boolean) {
        gen.writeBoolean((Boolean) value);
      } else if (value instanceof Double) {
        gen.writeNumber((Double) value);
      } else if (value instanceof Float) {
        gen.writeNumber((Float) value);
      } else {
        gen.writeString(value.toString());
      }
    }
  }

  // Enhanced expression parser that supports field IDs
  private Expression parseEnhancedExpression(JsonNode node) {
    if (node.isBoolean()) {
      return node.asBoolean() ? Expressions.alwaysTrue() : Expressions.alwaysFalse();
    }

    if (!node.isObject()) {
      throw new IllegalArgumentException("Cannot parse expression from: " + node);
    }

    String type = JsonUtil.getString("type", node);
    switch (type) {
      case "and":
        return Expressions.and(
            parseEnhancedExpression(JsonUtil.get("left", node)),
            parseEnhancedExpression(JsonUtil.get("right", node)));
      case "or":
        return Expressions.or(
            parseEnhancedExpression(JsonUtil.get("left", node)),
            parseEnhancedExpression(JsonUtil.get("right", node)));
      case "not":
        return Expressions.not(parseEnhancedExpression(JsonUtil.get("child", node)));
      case "eq":
        return Expressions.equal(
            parseEnhancedTerm(JsonUtil.get("term", node)), parseValue(JsonUtil.get("value", node)));
      case "gt":
        return Expressions.greaterThan(
            parseEnhancedTerm(JsonUtil.get("term", node)), parseValue(JsonUtil.get("value", node)));
      case "is-null":
        return Expressions.isNull(parseEnhancedTerm(JsonUtil.get("term", node)));
      default:
        throw new IllegalArgumentException("Unknown expression type: " + type);
    }
  }

  @SuppressWarnings("unchecked")
  private <T> UnboundTerm<T> parseEnhancedTerm(JsonNode node) {
    if (node.isTextual()) {
      return Expressions.ref(node.asText());
    }

    if (node.isObject()) {
      String type = JsonUtil.getString("type", node);
      if (REFERENCE_WITH_ID.equals(type)) {
        String name = JsonUtil.getString("name", node);
        int fieldId = JsonUtil.getInt(FIELD_ID, node);
        return Expressions.ref(name, fieldId);
      }
    }

    throw new IllegalArgumentException("Cannot parse term from: " + node);
  }

  private Object parseValue(JsonNode node) {
    if (node.isTextual()) {
      return node.asText();
    } else if (node.isLong()) {
      return node.asLong();
    } else if (node.isInt()) {
      return node.asInt();
    } else if (node.isBoolean()) {
      return node.asBoolean();
    } else if (node.isDouble()) {
      return node.asDouble();
    }
    return node.asText();
  }
}
