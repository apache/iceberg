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
package org.apache.iceberg;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.List;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.JsonUtil;

public class ConstraintParser {
  private ConstraintParser() {}

  private static final String CONSTRAINT_ID = "constraint-id";
  private static final String TYPE = "type";
  private static final String NAME = "name";
  private static final String ENFORCED = "enforced";
  private static final String TIMESTAMP_MS = "timestamp-ms";
  private static final String EXPRESSION = "expression";
  private static final String FIELD_IDS = "field-ids";

  public static String toJson(Constraint constraint) {
    return toJson(constraint, true);
  }

  public static String toJson(Constraint constraint, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(constraint, gen), pretty);
  }

  static void toJson(Constraint constraint, JsonGenerator generator) throws IOException {
    Preconditions.checkArgument(constraint != null, "Invalid constraint: null");

    generator.writeStartObject();
    generator.writeNumberField(CONSTRAINT_ID, constraint.id());
    generator.writeStringField(TYPE, constraint.type().typeName());
    generator.writeStringField(NAME, constraint.name());
    generator.writeBooleanField(ENFORCED, constraint.enforced());
    generator.writeNumberField(TIMESTAMP_MS, constraint.timestampMillis());

    if (constraint instanceof BaseConstraint.Check) {
      generator.writeFieldName(EXPRESSION);
      JsonUtil.mapper()
          .writeTree(
              generator,
              JsonUtil.mapper().readTree(((BaseConstraint.Check) constraint).expressionJson()));
    } else if (constraint instanceof KeyConstraint) {
      JsonUtil.writeIntegerArray(FIELD_IDS, ((KeyConstraint) constraint).fieldIds(), generator);
    } else {
      throw new IllegalArgumentException("Cannot serialize constraint: " + constraint);
    }

    generator.writeEndObject();
  }

  public static Constraint fromJson(String json) {
    return JsonUtil.parse(json, ConstraintParser::fromJson);
  }

  static Constraint fromJson(JsonNode node) {
    Preconditions.checkArgument(node != null, "Cannot parse constraint from null object");
    Preconditions.checkArgument(
        node.isObject(), "Cannot parse constraint from non-object: %s", node);

    int constraintId = JsonUtil.getInt(CONSTRAINT_ID, node);
    ConstraintType type = ConstraintType.fromTypeName(JsonUtil.getString(TYPE, node));
    String name = JsonUtil.getString(NAME, node);
    boolean enforced = JsonUtil.getBool(ENFORCED, node);
    long timestampMillis = JsonUtil.getLong(TIMESTAMP_MS, node);

    boolean hasExpression = node.hasNonNull(EXPRESSION);
    boolean hasFieldIds = node.hasNonNull(FIELD_IDS);

    switch (type) {
      case CHECK:
        Preconditions.checkArgument(
            !hasFieldIds, "Invalid check constraint %s: field IDs must not be set", name);
        Preconditions.checkArgument(
            hasExpression, "Invalid check constraint %s: missing expression", name);
        return BaseConstraint.check(
            constraintId, name, enforced, timestampMillis, node.get(EXPRESSION).toString());
      case UNIQUE:
        return BaseConstraint.unique(
            constraintId, name, enforced, timestampMillis, keyFieldIds(type, name, node));
      case PRIMARY_KEY:
        return BaseConstraint.primaryKey(
            constraintId, name, enforced, timestampMillis, keyFieldIds(type, name, node));
      default:
        throw new IllegalArgumentException("Unsupported constraint type: " + type);
    }
  }

  private static List<Integer> keyFieldIds(ConstraintType type, String name, JsonNode node) {
    Preconditions.checkArgument(
        !node.hasNonNull(EXPRESSION),
        "Invalid %s constraint %s: expression must not be set",
        type,
        name);
    Preconditions.checkArgument(
        node.hasNonNull(FIELD_IDS), "Invalid %s constraint %s: missing field IDs", type, name);
    return JsonUtil.getIntegerList(FIELD_IDS, node);
  }
}
