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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Locale;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SingleValueParser;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.JsonUtil;

public class ExpressionParser {

  private static final String TYPE = "type";
  private static final String VALUE = "value";
  private static final String VALUES = "values";
  private static final String TRANSFORM = "transform";
  private static final String TERM = "term";
  private static final String LEFT = "left";
  private static final String RIGHT = "right";
  private static final String CHILD = "child";
  private static final String REFERENCE = "reference";
  private static final String SOURCE_ID = "source-id";
  private static final String LITERAL = "literal";

  private enum SerializationMode {
    NAMES_ONLY,
    WITH_FIELD_IDS
  }

  private ExpressionParser() {}

  public static String toJson(Expression expression) {
    return toJson(expression, false);
  }

  public static String toJson(Expression expression, boolean pretty) {
    Preconditions.checkArgument(expression != null, "Invalid expression: null");
    return JsonUtil.generate(gen -> toJson(expression, gen), pretty);
  }

  public static String toResolvedJson(Expression expression) {
    return toResolvedJson(expression, false);
  }

  public static String toResolvedJson(Expression expression, boolean pretty) {
    Preconditions.checkArgument(expression != null, "Invalid expression: null");
    return JsonUtil.generate(
        gen -> toJsonInternal(expression, gen, SerializationMode.WITH_FIELD_IDS), pretty);
  }

  public static void toJson(Expression expression, JsonGenerator gen) {
    toJsonInternal(expression, gen, SerializationMode.NAMES_ONLY);
  }

  public static void toResolvedJson(Expression expression, JsonGenerator gen) {
    toJsonInternal(expression, gen, SerializationMode.WITH_FIELD_IDS);
  }

  private static void toJsonInternal(
      Expression expression, JsonGenerator gen, SerializationMode mode) {
    ExpressionVisitors.visit(expression, new JsonGeneratorVisitor(gen, mode));
  }

  private static class JsonGeneratorVisitor
      extends ExpressionVisitors.CustomOrderExpressionVisitor<Void> {
    private final JsonGenerator gen;
    private final SerializationMode mode;

    private JsonGeneratorVisitor(JsonGenerator gen, SerializationMode mode) {
      this.gen = gen;
      this.mode = mode;
    }

    /**
     * A convenience method to make code more readable by calling {@code toJson} instead of {@code
     * get()}
     */
    private void toJson(Supplier<Void> child) {
      child.get();
    }

    @FunctionalInterface
    private interface Task {
      void run() throws IOException;
    }

    private Void generate(Task task) {
      try {
        task.run();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }

      return null;
    }

    @Override
    public Void alwaysTrue() {
      return generate(() -> gen.writeBoolean(true));
    }

    @Override
    public Void alwaysFalse() {
      return generate(() -> gen.writeBoolean(false));
    }

    @Override
    public Void not(Supplier<Void> child) {
      return generate(
          () -> {
            gen.writeStartObject();
            gen.writeStringField(TYPE, "not");
            gen.writeFieldName(CHILD);
            toJson(child);
            gen.writeEndObject();
          });
    }

    @Override
    public Void and(Supplier<Void> left, Supplier<Void> right) {
      return generate(
          () -> {
            gen.writeStartObject();
            gen.writeStringField(TYPE, "and");
            gen.writeFieldName(LEFT);
            toJson(left);
            gen.writeFieldName(RIGHT);
            toJson(right);
            gen.writeEndObject();
          });
    }

    @Override
    public Void or(Supplier<Void> left, Supplier<Void> right) {
      return generate(
          () -> {
            gen.writeStartObject();
            gen.writeStringField(TYPE, "or");
            gen.writeFieldName(LEFT);
            toJson(left);
            gen.writeFieldName(RIGHT);
            toJson(right);
            gen.writeEndObject();
          });
    }

    @Override
    public <T> Void predicate(BoundPredicate<T> pred) {
      return generate(
          () -> {
            gen.writeStartObject();

            gen.writeStringField(TYPE, operationType(pred.op()));
            gen.writeFieldName(TERM);
            term(pred.term());

            if (pred.isLiteralPredicate()) {
              gen.writeFieldName(VALUE);
              SingleValueParser.toJson(
                  pred.term().type(), pred.asLiteralPredicate().literal().value(), gen);
            } else if (pred.isSetPredicate()) {
              gen.writeArrayFieldStart(VALUES);
              for (T value : pred.asSetPredicate().literalSet()) {
                SingleValueParser.toJson(pred.term().type(), value, gen);
              }
              gen.writeEndArray();
            }

            gen.writeEndObject();
          });
    }

    @Override
    public <T> Void predicate(UnboundPredicate<T> pred) {
      return generate(
          () -> {
            gen.writeStartObject();

            gen.writeStringField(TYPE, operationType(pred.op()));
            gen.writeFieldName(TERM);
            term(pred.term());

            if (pred.literals() != null) {
              if (pred.op() == Expression.Operation.IN
                  || pred.op() == Expression.Operation.NOT_IN) {
                gen.writeArrayFieldStart(VALUES);
                for (Literal<T> lit : pred.literals()) {
                  unboundLiteral(lit.value());
                }
                gen.writeEndArray();

              } else {
                gen.writeFieldName(VALUE);
                unboundLiteral(pred.literal().value());
              }
            }

            gen.writeEndObject();
          });
    }

    private void unboundLiteral(Object object) throws IOException {
      // this handles each type supported in Literals.from
      if (object instanceof Integer) {
        SingleValueParser.toJson(Types.IntegerType.get(), object, gen);
      } else if (object instanceof Long) {
        SingleValueParser.toJson(Types.LongType.get(), object, gen);
      } else if (object instanceof String) {
        SingleValueParser.toJson(Types.StringType.get(), object, gen);
      } else if (object instanceof Float) {
        SingleValueParser.toJson(Types.FloatType.get(), object, gen);
      } else if (object instanceof Double) {
        SingleValueParser.toJson(Types.DoubleType.get(), object, gen);
      } else if (object instanceof Boolean) {
        SingleValueParser.toJson(Types.BooleanType.get(), object, gen);
      } else if (object instanceof ByteBuffer) {
        SingleValueParser.toJson(Types.BinaryType.get(), object, gen);
      } else if (object instanceof byte[]) {
        SingleValueParser.toJson(Types.BinaryType.get(), ByteBuffer.wrap((byte[]) object), gen);
      } else if (object instanceof UUID) {
        SingleValueParser.toJson(Types.UUIDType.get(), object, gen);
      } else if (object instanceof BigDecimal) {
        BigDecimal decimal = (BigDecimal) object;
        SingleValueParser.toJson(
            Types.DecimalType.of(decimal.precision(), decimal.scale()), decimal, gen);
      }
    }

    private String operationType(Expression.Operation op) {
      return op.toString().replaceAll("_", "-").toLowerCase(Locale.ENGLISH);
    }

    private void term(Term term) throws IOException {
      if (term instanceof UnboundTransform) {
        UnboundTransform<?, ?> transform = (UnboundTransform<?, ?>) term;
        NamedReference<?> ref = transform.ref();
        Integer fieldId = null;
        if (mode == SerializationMode.WITH_FIELD_IDS && ref instanceof IDReference) {
          fieldId = ((IDReference<?>) ref).id();
        }
        transform(transform.transform().toString(), ref.name(), fieldId);
        return;
      } else if (term instanceof BoundTransform) {
        BoundTransform<?, ?> transform = (BoundTransform<?, ?>) term;
        Integer fieldId =
            mode == SerializationMode.WITH_FIELD_IDS ? transform.ref().fieldId() : null;
        transform(transform.transform().toString(), transform.ref().name(), fieldId);
        return;
      } else if (term instanceof BoundReference) {
        BoundReference<?> ref = (BoundReference<?>) term;
        Integer fieldId = mode == SerializationMode.WITH_FIELD_IDS ? ref.fieldId() : null;
        reference(ref.name(), fieldId);
        return;
      } else if (term instanceof IDReference) {
        IDReference<?> ref = (IDReference<?>) term;
        Integer fieldId = mode == SerializationMode.WITH_FIELD_IDS ? ref.id() : null;
        reference(ref.name(), fieldId);
        return;
      } else if (term instanceof Reference) {
        gen.writeString(((Reference<?>) term).name());
        return;
      }

      throw new UnsupportedOperationException("Cannot write unsupported term: " + term);
    }

    private void transform(String transform, String name, Integer fieldId) throws IOException {
      gen.writeStartObject();
      gen.writeStringField(TYPE, TRANSFORM);
      gen.writeStringField(TRANSFORM, transform);
      gen.writeFieldName(TERM);
      if (fieldId != null) {
        reference(name, fieldId);
      } else {
        gen.writeString(name);
      }
      gen.writeEndObject();
    }

    private void reference(String name, Integer fieldId) throws IOException {
      if (fieldId != null) {
        gen.writeStartObject();
        gen.writeStringField(TYPE, REFERENCE);
        if (name != null) {
          gen.writeStringField(TERM, name);
        }
        gen.writeNumberField(SOURCE_ID, fieldId);
        gen.writeEndObject();
      } else {
        gen.writeString(name);
      }
    }
  }

  public static Expression fromJson(String json) {
    return fromJson(json, null);
  }

  public static Expression fromJson(JsonNode json) {
    return fromJson(json, null);
  }

  public static Expression fromJson(String json, Schema schema) {
    return JsonUtil.parse(json, node -> fromJson(node, schema));
  }

  static Expression fromJson(JsonNode json, Schema schema) {
    Preconditions.checkArgument(null != json, "Cannot parse expression from null object");
    // check for constant expressions
    if (json.isBoolean()) {
      if (json.asBoolean()) {
        return Expressions.alwaysTrue();
      } else {
        return Expressions.alwaysFalse();
      }
    }

    Preconditions.checkArgument(
        json.isObject(), "Cannot parse expression from non-object: %s", json);

    String type = JsonUtil.getString(TYPE, json);
    if (type.equalsIgnoreCase(LITERAL)) {
      if (JsonUtil.getBool(VALUE, json)) {
        return Expressions.alwaysTrue();
      } else {
        return Expressions.alwaysFalse();
      }
    }

    Expression.Operation op = fromType(type);
    switch (op) {
      case NOT:
        return Expressions.not(fromJson(JsonUtil.get(CHILD, json), schema));
      case AND:
        return Expressions.and(
            fromJson(JsonUtil.get(LEFT, json), schema),
            fromJson(JsonUtil.get(RIGHT, json), schema));
      case OR:
        return Expressions.or(
            fromJson(JsonUtil.get(LEFT, json), schema),
            fromJson(JsonUtil.get(RIGHT, json), schema));
    }

    return predicateFromJson(op, json, schema);
  }

  private static Expression.Operation fromType(String type) {
    return Expression.Operation.fromString(type.replaceAll("-", "_"));
  }

  @SuppressWarnings("unchecked")
  private static <T> UnboundPredicate<T> predicateFromJson(
      Expression.Operation op, JsonNode node, Schema schema) {
    UnboundTerm<T> term = term(JsonUtil.get(TERM, node));

    Function<JsonNode, T> convertValue;
    if (schema != null) {
      BoundTerm<?> bound = term.bind(schema.asStruct(), false);
      convertValue = valueNode -> (T) SingleValueParser.fromJson(bound.type(), valueNode);
    } else {
      convertValue = valueNode -> (T) ExpressionParser.asObject(valueNode);
    }

    switch (op) {
      case IS_NULL:
      case NOT_NULL:
      case IS_NAN:
      case NOT_NAN:
        // unary predicates
        Preconditions.checkArgument(
            !node.has(VALUE), "Cannot parse %s predicate: has invalid value field", op);
        Preconditions.checkArgument(
            !node.has(VALUES), "Cannot parse %s predicate: has invalid values field", op);
        return Expressions.predicate(op, term);
      case LT:
      case LT_EQ:
      case GT:
      case GT_EQ:
      case EQ:
      case NOT_EQ:
      case STARTS_WITH:
      case NOT_STARTS_WITH:
        // literal predicates
        Preconditions.checkArgument(
            node.has(VALUE), "Cannot parse %s predicate: missing value", op);
        Preconditions.checkArgument(
            !node.has(VALUES), "Cannot parse %s predicate: has invalid values field", op);
        T value = literal(JsonUtil.get(VALUE, node), convertValue);
        return Expressions.predicate(op, term, ImmutableList.of(value));
      case IN:
      case NOT_IN:
        // literal set predicates
        Preconditions.checkArgument(
            node.has(VALUES), "Cannot parse %s predicate: missing values", op);
        Preconditions.checkArgument(
            !node.has(VALUE), "Cannot parse %s predicate: has invalid value field", op);
        JsonNode valuesNode = JsonUtil.get(VALUES, node);
        Preconditions.checkArgument(
            valuesNode.isArray(), "Cannot parse literals from non-array: %s", valuesNode);
        return Expressions.predicate(
            op,
            term,
            Iterables.transform(
                ((ArrayNode) valuesNode)::elements, valueNode -> literal(valueNode, convertValue)));
      default:
        throw new UnsupportedOperationException("Unsupported operation: " + op);
    }
  }

  private static <T> T literal(JsonNode valueNode, Function<JsonNode, T> toValue) {
    if (valueNode.isObject() && valueNode.has(TYPE)) {
      String type = JsonUtil.getString(TYPE, valueNode);
      Preconditions.checkArgument(
          type.equalsIgnoreCase(LITERAL), "Cannot parse type as a literal: %s", type);
      return toValue.apply(JsonUtil.get(VALUE, valueNode));
    }

    // the node is a directly embedded literal value
    return toValue.apply(valueNode);
  }

  private static Object asObject(JsonNode node) {
    if (node.isIntegralNumber() && node.canConvertToLong()) {
      return node.asLong();
    } else if (node.isTextual()) {
      return node.asText();
    } else if (node.isFloatingPointNumber()) {
      return node.asDouble();
    } else if (node.isBoolean()) {
      return node.asBoolean();
    } else {
      throw new IllegalArgumentException("Cannot convert JSON to literal: " + node);
    }
  }

  @SuppressWarnings("unchecked")
  private static <T> UnboundTerm<T> term(JsonNode node) {
    if (node.isTextual()) {
      return Expressions.ref(node.asText());
    } else if (node.isObject()) {
      String type = JsonUtil.getString(TYPE, node);
      switch (type) {
        case REFERENCE:
          // source-id is optional - if present, creates IDReference, otherwise NamedReference
          String name = JsonUtil.getString(TERM, node);
          boolean hasSourceId = node.has(SOURCE_ID);

          if (hasSourceId) {
            int fieldId = JsonUtil.getInt(SOURCE_ID, node);
            return Expressions.ref(name, fieldId);
          } else {
            return Expressions.ref(name);
          }
        case TRANSFORM:
          UnboundTerm<T> child = term(JsonUtil.get(TERM, node));
          String transformStr = JsonUtil.getString(TRANSFORM, node);
          NamedReference<?> ref;
          if (child instanceof NamedReference) {
            ref = (NamedReference<?>) child;
          } else {
            ref = child.ref();
          }
          Transform<?, ?> transform = Transforms.fromString(transformStr);
          return (UnboundTerm<T>) new UnboundTransform(ref, transform);
        default:
          throw new IllegalArgumentException("Cannot parse type as a reference: " + type);
      }
    }

    throw new IllegalArgumentException(
        "Cannot parse reference (requires string or object): " + node);
  }
}
