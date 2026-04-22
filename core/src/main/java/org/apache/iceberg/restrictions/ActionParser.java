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
package org.apache.iceberg.restrictions;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import org.apache.iceberg.expressions.ExpressionParser;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.JsonUtil;

public class ActionParser {

  private ActionParser() {}

  private static final String ACTION = "action";
  private static final String FIELD_ID = "field-id";
  private static final String EXPRESSION = "expression";

  public static String toJson(Action action) {
    return toJson(action, false);
  }

  public static String toJson(Action action, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(action, gen), pretty);
  }

  public static void toJson(Action action, JsonGenerator generator) throws IOException {
    Preconditions.checkArgument(action != null, "Invalid action: null");

    generator.writeStartObject();
    generator.writeStringField(ACTION, action.actionType());
    generator.writeNumberField(FIELD_ID, action.fieldId());

    if (action instanceof Action.ApplyExpression) {
      generator.writeFieldName(EXPRESSION);
      ExpressionParser.toJson(((Action.ApplyExpression) action).expression(), generator);
    }

    generator.writeEndObject();
  }

  public static Action fromJson(String json) {
    return JsonUtil.parse(json, ActionParser::fromJson);
  }

  public static Action fromJson(JsonNode node) {
    Preconditions.checkArgument(
        node != null && node.isObject(), "Cannot parse action from non-object value: %s", node);
    Preconditions.checkArgument(
        node.hasNonNull(ACTION), "Cannot parse action: missing field: action");
    Preconditions.checkArgument(
        node.hasNonNull(FIELD_ID), "Cannot parse action: missing field: field-id");

    String actionType = JsonUtil.getString(ACTION, node);
    int fieldId = JsonUtil.getInt(FIELD_ID, node);

    switch (actionType) {
      case Action.MASK_ALPHANUM:
        return new Action.MaskAlphanum(fieldId);
      case Action.MASK_TO_DEFAULT:
        return new Action.MaskToDefault(fieldId);
      case Action.REPLACE_WITH_NULL:
        return new Action.ReplaceWithNull(fieldId);
      case Action.SHOW_FIRST_4:
        return new Action.ShowFirst4(fieldId);
      case Action.SHOW_LAST_4:
        return new Action.ShowLast4(fieldId);
      case Action.TRUNCATE_TO_YEAR:
        return new Action.TruncateToYear(fieldId);
      case Action.TRUNCATE_TO_MONTH:
        return new Action.TruncateToMonth(fieldId);
      case Action.SHA_256_GLOBAL:
        return new Action.Sha256Global(fieldId);
      case Action.SHA_256_QUERY_LOCAL:
        return new Action.Sha256QueryLocal(fieldId);
      case Action.APPLY_EXPRESSION:
        Preconditions.checkArgument(
            node.hasNonNull(EXPRESSION),
            "Cannot parse apply-expression action: missing field: expression");
        return new Action.ApplyExpression(fieldId, ExpressionParser.fromJson(node.get(EXPRESSION)));
      default:
        // Preserve unknown action types so older clients don't break when the spec evolves.
        // Enforcement code (engine rules) must fail closed on Action.Unknown, since silently
        // skipping an unknown mask would leak unmasked data.
        return new Action.Unknown(fieldId, actionType);
    }
  }
}
