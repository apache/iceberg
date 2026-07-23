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
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SingleValueParser;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
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
  private static final String LITERAL = "literal";
  private static final String APPLY = "apply";
  private static final String FUNCTION = "function";
  private static final String ARGUMENTS = "arguments";
  private static final String NAME = "name";
  private static final String ID = "id";
  private static final String IDENTIFIER = "identifier";
  private static final String CATALOG = "catalog";

  private static final Pattern HAS_WIDTH = Pattern.compile("(\\w+)\\[(\\d+)]");

  private ExpressionParser() {}

  public static String toJson(Expression expression) {
    return toJson(expression, false);
  }

  public static String toJson(Expression expression, boolean pretty) {
    Preconditions.checkArgument(expression != null, "Invalid expression: null");
    return JsonUtil.generate(gen -> toJson(expression, gen), pretty);
  }

  public static void toJson(Expression expression, JsonGenerator gen) {
    ExpressionVisitors.visit(expression, new JsonGeneratorVisitor(gen));
  }

  private static class JsonGeneratorVisitor
      extends ExpressionVisitors.CustomOrderExpressionVisitor<Void> {
    private final JsonGenerator gen;

    private JsonGeneratorVisitor(JsonGenerator gen) {
      this.gen = gen;
    }

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

            if (pred.isUnaryPredicate()) {
              gen.writeFieldName(CHILD);
              writeValueExpr(pred.term());
            } else if (pred.isLiteralPredicate()) {
              gen.writeFieldName(LEFT);
              writeValueExpr(pred.term());
              gen.writeFieldName(RIGHT);
              SingleValueParser.toJson(
                  pred.term().type(), pred.asLiteralPredicate().literal().value(), gen);
            } else if (pred.isSetPredicate()) {
              gen.writeFieldName(CHILD);
              writeValueExpr(pred.term());
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

            if (pred.literals() == null || pred.literals().isEmpty()) {
              gen.writeFieldName(CHILD);
              writeValueExpr(pred.term());
            } else if (pred.op() == Expression.Operation.IN
                || pred.op() == Expression.Operation.NOT_IN) {
              gen.writeFieldName(CHILD);
              writeValueExpr(pred.term());
              gen.writeArrayFieldStart(VALUES);
              for (Literal<T> lit : pred.literals()) {
                unboundLiteral(lit.value());
              }
              gen.writeEndArray();
            } else {
              gen.writeFieldName(LEFT);
              writeValueExpr(pred.term());
              gen.writeFieldName(RIGHT);
              unboundLiteral(pred.literal().value());
            }

            gen.writeEndObject();
          });
    }

    private void unboundLiteral(Object object) throws IOException {
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

    private void writeValueExpr(Term term) throws IOException {
      if (term instanceof UnboundApply) {
        writeApply((UnboundApply<?>) term);
      } else if (term instanceof UnboundTransform) {
        writeTransformAsApply((UnboundTransform<?, ?>) term);
      } else if (term instanceof BoundTransform) {
        writeBoundTransformAsApply((BoundTransform<?, ?>) term);
      } else if (term instanceof BoundReference) {
        BoundReference<?> ref = (BoundReference<?>) term;
        gen.writeStartObject();
        gen.writeStringField(TYPE, REFERENCE);
        gen.writeNumberField(ID, ref.fieldId());
        gen.writeEndObject();
      } else if (term instanceof Reference) {
        gen.writeStartObject();
        gen.writeStringField(TYPE, REFERENCE);
        gen.writeStringField(NAME, ((Reference<?>) term).name());
        gen.writeEndObject();
      } else {
        throw new UnsupportedOperationException("Cannot write unsupported term: " + term);
      }
    }

    private void writeTransformAsApply(UnboundTransform<?, ?> transform) throws IOException {
      gen.writeStartObject();
      gen.writeStringField(TYPE, APPLY);

      String transformStr = transform.transform().toString();
      writeTransformFunction(transformStr);

      gen.writeArrayFieldStart(ARGUMENTS);
      Matcher matcher = HAS_WIDTH.matcher(transformStr);
      if (matcher.matches()) {
        gen.writeNumber(Integer.parseInt(matcher.group(2)));
      }
      writeValueExpr(transform.ref());
      gen.writeEndArray();

      gen.writeEndObject();
    }

    private void writeBoundTransformAsApply(BoundTransform<?, ?> transform) throws IOException {
      gen.writeStartObject();
      gen.writeStringField(TYPE, APPLY);

      String transformStr = transform.transform().toString();
      writeTransformFunction(transformStr);

      gen.writeArrayFieldStart(ARGUMENTS);
      Matcher matcher = HAS_WIDTH.matcher(transformStr);
      if (matcher.matches()) {
        gen.writeNumber(Integer.parseInt(matcher.group(2)));
      }
      writeValueExpr(transform.ref());
      gen.writeEndArray();

      gen.writeEndObject();
    }

    private void writeTransformFunction(String transformStr) throws IOException {
      Matcher matcher = HAS_WIDTH.matcher(transformStr);
      String functionName;
      if (matcher.matches()) {
        functionName = matcher.group(1);
      } else {
        functionName = transformStr;
      }
      gen.writeStringField(FUNCTION, functionName);
    }

    private void writeApply(UnboundApply<?> apply) throws IOException {
      gen.writeStartObject();
      gen.writeStringField(TYPE, APPLY);

      writeFunctionRef(apply.function());

      gen.writeArrayFieldStart(ARGUMENTS);
      for (Object arg : apply.arguments()) {
        if (arg instanceof UnboundTerm) {
          writeValueExpr((Term) arg);
        } else if (arg instanceof Expression) {
          ExpressionParser.toJson((Expression) arg, gen);
        } else if (arg instanceof Number) {
          gen.writeNumber(((Number) arg).intValue());
        } else {
          throw new UnsupportedOperationException("Cannot write apply argument: " + arg);
        }
      }
      gen.writeEndArray();

      gen.writeEndObject();
    }

    private void writeFunctionRef(FunctionReference ref) throws IOException {
      if (ref.catalog() == null && ref.identifier().size() == 1) {
        gen.writeStringField(FUNCTION, ref.name());
      } else if (ref.catalog() == null) {
        gen.writeFieldName(FUNCTION);
        gen.writeStartArray();
        for (String part : ref.identifier()) {
          gen.writeString(part);
        }
        gen.writeEndArray();
      } else {
        gen.writeFieldName(FUNCTION);
        gen.writeStartObject();
        gen.writeStringField(CATALOG, ref.catalog());
        gen.writeFieldName(IDENTIFIER);
        gen.writeStartArray();
        for (String part : ref.identifier()) {
          gen.writeString(part);
        }
        gen.writeEndArray();
        gen.writeEndObject();
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

    if (json.has(TERM)) {
      return predicateFromJsonDeprecated(op, json, schema);
    } else {
      return predicateFromJsonNew(op, json, schema);
    }
  }

  private static Expression.Operation fromType(String type) {
    return Expression.Operation.fromString(type.replaceAll("-", "_"));
  }

  @SuppressWarnings("unchecked")
  private static <T> UnboundPredicate<T> predicateFromJsonNew(
      Expression.Operation op, JsonNode node, Schema schema) {
    switch (op) {
      case IS_NULL:
      case NOT_NULL:
      case IS_NAN:
      case NOT_NAN:
        {
          UnboundTerm<T> child = valueExprFromJson(JsonUtil.get(CHILD, node));
          return Expressions.predicate(op, child);
        }
      case LT:
      case LT_EQ:
      case GT:
      case GT_EQ:
      case EQ:
      case NOT_EQ:
      case STARTS_WITH:
      case NOT_STARTS_WITH:
        {
          UnboundTerm<T> left = valueExprFromJson(JsonUtil.get(LEFT, node));
          Function<JsonNode, T> convertValue = valueConverter(left, schema);
          T value = literalFromJson(JsonUtil.get(RIGHT, node), convertValue);
          return Expressions.predicate(op, left, ImmutableList.of(value));
        }
      case IN:
      case NOT_IN:
        {
          UnboundTerm<T> child = valueExprFromJson(JsonUtil.get(CHILD, node));
          Function<JsonNode, T> convertValue = valueConverter(child, schema);
          JsonNode valuesNode = JsonUtil.get(VALUES, node);
          Iterable<T> values = literalsFromJson(valuesNode, convertValue);
          return Expressions.predicate(op, child, values);
        }
      default:
        throw new UnsupportedOperationException("Unsupported operation: " + op);
    }
  }

  @SuppressWarnings("unchecked")
  private static <T> UnboundPredicate<T> predicateFromJsonDeprecated(
      Expression.Operation op, JsonNode node, Schema schema) {
    UnboundTerm<T> term = termFromJsonDeprecated(JsonUtil.get(TERM, node));

    Function<JsonNode, T> convertValue = valueConverter(term, schema);

    switch (op) {
      case IS_NULL:
      case NOT_NULL:
      case IS_NAN:
      case NOT_NAN:
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
        Preconditions.checkArgument(
            node.has(VALUE), "Cannot parse %s predicate: missing value", op);
        Preconditions.checkArgument(
            !node.has(VALUES), "Cannot parse %s predicate: has invalid values field", op);
        T value = literalFromJson(JsonUtil.get(VALUE, node), convertValue);
        return Expressions.predicate(op, term, ImmutableList.of(value));
      case IN:
      case NOT_IN:
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
                ((ArrayNode) valuesNode)::elements,
                valueNode -> literalFromJson(valueNode, convertValue)));
      default:
        throw new UnsupportedOperationException("Unsupported operation: " + op);
    }
  }

  @SuppressWarnings("unchecked")
  private static <T> Function<JsonNode, T> valueConverter(UnboundTerm<T> term, Schema schema) {
    if (schema != null) {
      BoundTerm<?> bound = term.bind(schema.asStruct(), false);
      return valueNode -> (T) SingleValueParser.fromJson(bound.type(), valueNode);
    } else {
      return valueNode -> (T) ExpressionParser.asObject(valueNode);
    }
  }

  @SuppressWarnings("unchecked")
  private static <T> UnboundTerm<T> valueExprFromJson(JsonNode node) {
    if (node.isTextual()) {
      return Expressions.ref(node.asText());
    }

    if (node.isObject()) {
      String type = JsonUtil.getString(TYPE, node);
      switch (type) {
        case REFERENCE:
          return referenceFromJson(node);
        case APPLY:
          return applyFromJson(node);
        default:
          throw new IllegalArgumentException("Unknown value expression type: " + type);
      }
    }

    throw new IllegalArgumentException("Cannot parse value expression: " + node);
  }

  @SuppressWarnings("unchecked")
  private static <T> UnboundTerm<T> referenceFromJson(JsonNode node) {
    if (node.has(NAME)) {
      return Expressions.ref(JsonUtil.getString(NAME, node));
    } else if (node.has(TERM)) {
      return Expressions.ref(JsonUtil.getString(TERM, node));
    }

    throw new IllegalArgumentException(
        "Cannot parse reference (requires 'name' or 'term' field): " + node);
  }

  @SuppressWarnings("unchecked")
  private static <T> UnboundTerm<T> applyFromJson(JsonNode node) {
    FunctionReference funcRef = functionRefFromJson(JsonUtil.get(FUNCTION, node));
    List<Object> arguments = Lists.newArrayList();

    if (node.has(ARGUMENTS)) {
      JsonNode argsNode = JsonUtil.get(ARGUMENTS, node);
      Preconditions.checkArgument(
          argsNode.isArray(), "Apply arguments must be an array: %s", argsNode);
      for (JsonNode argNode : argsNode) {
        arguments.add(parseApplyArgument(argNode));
      }
    }

    UnboundApply<T> apply = new UnboundApply<>(funcRef, arguments);

    if (apply.isKnownTransform()) {
      return apply.asTransform();
    }

    return apply;
  }

  private static Object parseApplyArgument(JsonNode node) {
    if (node.isIntegralNumber()) {
      return node.asInt();
    } else if (node.isFloatingPointNumber()) {
      return node.asDouble();
    } else if (node.isTextual()) {
      return Expressions.ref(node.asText());
    } else if (node.isBoolean()) {
      return node.asBoolean();
    } else if (node.isObject()) {
      String type = JsonUtil.getString(TYPE, node);
      switch (type) {
        case REFERENCE:
          return referenceFromJson(node);
        case APPLY:
          return applyFromJson(node);
        case LITERAL:
          return asObject(JsonUtil.get(VALUE, node));
        default:
          return fromJson(node, null);
      }
    }

    throw new IllegalArgumentException("Cannot parse apply argument: " + node);
  }

  private static FunctionReference functionRefFromJson(JsonNode node) {
    if (node.isTextual()) {
      return FunctionReference.of(node.asText());
    } else if (node.isArray()) {
      List<String> parts = Lists.newArrayList();
      for (JsonNode part : node) {
        parts.add(part.asText());
      }
      return FunctionReference.of(parts);
    } else if (node.isObject()) {
      String catalog = node.has(CATALOG) ? JsonUtil.getString(CATALOG, node) : null;
      JsonNode identifierNode = JsonUtil.get(IDENTIFIER, node);
      List<String> identifier = Lists.newArrayList();
      if (identifierNode.isArray()) {
        for (JsonNode part : identifierNode) {
          identifier.add(part.asText());
        }
      } else {
        identifier.add(identifierNode.asText());
      }
      return catalog != null
          ? FunctionReference.of(catalog, identifier)
          : FunctionReference.of(identifier);
    }

    throw new IllegalArgumentException("Cannot parse function reference: " + node);
  }

  private static <T> T literalFromJson(JsonNode valueNode, Function<JsonNode, T> toValue) {
    if (valueNode.isObject() && valueNode.has(TYPE)) {
      String type = JsonUtil.getString(TYPE, valueNode);
      Preconditions.checkArgument(
          type.equalsIgnoreCase(LITERAL), "Cannot parse type as a literal: %s", type);
      return toValue.apply(JsonUtil.get(VALUE, valueNode));
    }

    return toValue.apply(valueNode);
  }

  private static <T> Iterable<T> literalsFromJson(
      JsonNode node, Function<JsonNode, T> convertValue) {
    if (node.isArray()) {
      return Iterables.transform(
          ((ArrayNode) node)::elements, valueNode -> literalFromJson(valueNode, convertValue));
    } else if (node.isObject() && node.has(TYPE)) {
      String type = JsonUtil.getString(TYPE, node);
      Preconditions.checkArgument(
          type.equalsIgnoreCase("literals"), "Cannot parse type as literals: %s", type);
      JsonNode valuesNode = JsonUtil.get(VALUES, node);
      Preconditions.checkArgument(
          valuesNode.isArray(), "Cannot parse literals values from non-array: %s", valuesNode);
      return Iterables.transform(((ArrayNode) valuesNode)::elements, convertValue::apply);
    }

    throw new IllegalArgumentException("Cannot parse literals: " + node);
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
  private static <T> UnboundTerm<T> termFromJsonDeprecated(JsonNode node) {
    if (node.isTextual()) {
      return Expressions.ref(node.asText());
    } else if (node.isObject()) {
      String type = JsonUtil.getString(TYPE, node);
      switch (type) {
        case REFERENCE:
          if (node.has(NAME)) {
            return Expressions.ref(JsonUtil.getString(NAME, node));
          }
          return Expressions.ref(JsonUtil.getString(TERM, node));
        case TRANSFORM:
          UnboundTerm<T> child = termFromJsonDeprecated(JsonUtil.get(TERM, node));
          String transform = JsonUtil.getString(TRANSFORM, node);
          return (UnboundTerm<T>)
              Expressions.transform(child.ref().name(), Transforms.fromString(transform));
        default:
          throw new IllegalArgumentException("Cannot parse type as a reference: " + type);
      }
    }

    throw new IllegalArgumentException(
        "Cannot parse reference (requires string or object): " + node);
  }
}
