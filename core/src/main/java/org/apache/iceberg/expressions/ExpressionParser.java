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
import org.apache.iceberg.geospatial.GeospatialBound;
import org.apache.iceberg.geospatial.GeospatialBoundingBox;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
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
            } else if (pred.isGeospatialPredicate()) {
              gen.writeFieldName(VALUE);
              geospatialBoundingBox(pred.asGeospatialPredicate().literal().value());
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

              } else if (pred.op() == Expression.Operation.ST_INTERSECTS
                  || pred.op() == Expression.Operation.ST_DISJOINT) {
                gen.writeFieldName(VALUE);
                Literal<ByteBuffer> min = pred.literals().get(0).to(Types.BinaryType.get());
                Literal<ByteBuffer> max = pred.literals().get(1).to(Types.BinaryType.get());
                GeospatialBoundingBox bbox = GeospatialBoundingBox.create(min.value(), max.value());
                geospatialBoundingBox(bbox);
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

    private void geospatialBoundingBox(GeospatialBoundingBox value) throws IOException {
      gen.writeStartObject();

      // Write x coordinate
      gen.writeFieldName("x");
      gen.writeStartObject();
      gen.writeNumberField("min", value.min().x());
      gen.writeNumberField("max", value.max().x());
      gen.writeEndObject();

      // Write y coordinate
      gen.writeFieldName("y");
      gen.writeStartObject();
      gen.writeNumberField("min", value.min().y());
      gen.writeNumberField("max", value.max().y());
      gen.writeEndObject();

      // Write z coordinate if present
      if (value.min().hasZ() || value.max().hasZ()) {
        gen.writeFieldName("z");
        gen.writeStartObject();
        gen.writeNumberField("min", value.min().z());
        gen.writeNumberField("max", value.max().z());
        gen.writeEndObject();
      }

      // Write m coordinate if present
      if (value.min().hasM() || value.max().hasM()) {
        gen.writeFieldName("m");
        gen.writeStartObject();
        gen.writeNumberField("min", value.min().m());
        gen.writeNumberField("max", value.max().m());
        gen.writeEndObject();
      }

      gen.writeEndObject();
    }

    private String operationType(Expression.Operation op) {
      return op.toString().replaceAll("_", "-").toLowerCase(Locale.ENGLISH);
    }

    private void term(Term term) throws IOException {
      if (term instanceof UnboundTransform) {
        UnboundTransform<?, ?> transform = (UnboundTransform<?, ?>) term;
        transform(transform.transform().toString(), transform.ref().name());
        return;
      } else if (term instanceof BoundTransform) {
        BoundTransform<?, ?> transform = (BoundTransform<?, ?>) term;
        transform(transform.transform().toString(), transform.ref().name());
        return;
      } else if (term instanceof Reference) {
        gen.writeString(((Reference<?>) term).name());
        return;
      }

      throw new UnsupportedOperationException("Cannot write unsupported term: " + term);
    }

    private void transform(String transform, String name) throws IOException {
      gen.writeStartObject();
      gen.writeStringField(TYPE, TRANSFORM);
      gen.writeStringField(TRANSFORM, transform);
      gen.writeStringField(TERM, name);
      gen.writeEndObject();
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

      case ST_INTERSECTS:
      case ST_DISJOINT:
        return geospatialPredicateFromJson(op, json);
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

  private static Expression geospatialPredicateFromJson(Expression.Operation op, JsonNode node) {
    UnboundTerm<ByteBuffer> term = term(JsonUtil.get(TERM, node));
    Preconditions.checkArgument(node.has(VALUE), "Cannot parse %s predicate: missing value", op);
    Preconditions.checkArgument(
        !node.has(VALUES), "Cannot parse %s predicate: has invalid values field", op);
    GeospatialBoundingBox geospatialBoundingBox = geospatialBoundingBox(JsonUtil.get(VALUE, node));
    return Expressions.geospatialPredicate(op, term, geospatialBoundingBox);
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

  private static GeospatialBoundingBox geospatialBoundingBox(JsonNode valueNode) {
    // X and Y coordinates are required
    double xMin = valueNode.get("x").get("min").asDouble();
    double xMax = valueNode.get("x").get("max").asDouble();
    double yMin = valueNode.get("y").get("min").asDouble();
    double yMax = valueNode.get("y").get("max").asDouble();

    // Create GeospatialBound objects for min and max
    GeospatialBound minBound;
    GeospatialBound maxBound;

    // Check if Z coordinate exists
    boolean hasZ = valueNode.has("z");
    // Check if M coordinate exists
    boolean hasM = valueNode.has("m");

    if (hasZ && hasM) {
      // Both Z and M present
      double zMin = valueNode.get("z").get("min").asDouble();
      double zMax = valueNode.get("z").get("max").asDouble();
      double mMin = valueNode.get("m").get("min").asDouble();
      double mMax = valueNode.get("m").get("max").asDouble();
      minBound = GeospatialBound.createXYZM(xMin, yMin, zMin, mMin);
      maxBound = GeospatialBound.createXYZM(xMax, yMax, zMax, mMax);
    } else if (hasZ) {
      // Only Z present, no M
      double zMin = valueNode.get("z").get("min").asDouble();
      double zMax = valueNode.get("z").get("max").asDouble();
      minBound = GeospatialBound.createXYZ(xMin, yMin, zMin);
      maxBound = GeospatialBound.createXYZ(xMax, yMax, zMax);
    } else if (hasM) {
      // Only M present, no Z
      double mMin = valueNode.get("m").get("min").asDouble();
      double mMax = valueNode.get("m").get("max").asDouble();
      minBound = GeospatialBound.createXYM(xMin, yMin, mMin);
      maxBound = GeospatialBound.createXYM(xMax, yMax, mMax);
    } else {
      // Only X and Y present
      minBound = GeospatialBound.createXY(xMin, yMin);
      maxBound = GeospatialBound.createXY(xMax, yMax);
    }

    return new GeospatialBoundingBox(minBound, maxBound);
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
          return Expressions.ref(JsonUtil.getString(TERM, node));
        case TRANSFORM:
          UnboundTerm<T> child = term(JsonUtil.get(TERM, node));
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
