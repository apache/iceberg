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
package org.apache.iceberg.rest.restrictions;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import org.apache.iceberg.functions.IcebergFunction;
import org.apache.iceberg.functions.MaskAlphanum;
import org.apache.iceberg.functions.MaskToFixedValue;
import org.apache.iceberg.functions.ReplaceWithNull;
import org.apache.iceberg.functions.Sha256Global;
import org.apache.iceberg.functions.Sha256QueryLocal;
import org.apache.iceberg.functions.ShowFirst4;
import org.apache.iceberg.functions.ShowLast4;
import org.apache.iceberg.functions.TruncateToMonth;
import org.apache.iceberg.functions.TruncateToYear;
import org.apache.iceberg.functions.UnknownFunction;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.JsonUtil;

public class ActionParser {

  private ActionParser() {}

  private static final String ACTION = "action";
  private static final String FIELD_ID = "field-id";

  public static String toJson(IcebergFunction<?, ?> action) {
    return toJson(action, false);
  }

  public static String toJson(IcebergFunction<?, ?> action, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(action, gen), pretty);
  }

  public static void toJson(IcebergFunction<?, ?> action, JsonGenerator generator)
      throws IOException {
    Preconditions.checkArgument(action != null, "Invalid action: null");

    generator.writeStartObject();
    generator.writeStringField(ACTION, action.name());
    generator.writeNumberField(FIELD_ID, action.fieldId());
    generator.writeEndObject();
  }

  public static IcebergFunction<?, ?> fromJson(String json) {
    return JsonUtil.parse(json, ActionParser::fromJson);
  }

  public static IcebergFunction<?, ?> fromJson(JsonNode node) {
    Preconditions.checkArgument(
        node != null && node.isObject(), "Cannot parse action from non-object value: %s", node);
    Preconditions.checkArgument(
        node.hasNonNull(ACTION), "Cannot parse action: missing field: action");
    Preconditions.checkArgument(
        node.hasNonNull(FIELD_ID), "Cannot parse action: missing field: field-id");

    String actionType = JsonUtil.getString(ACTION, node);
    int fieldId = JsonUtil.getInt(FIELD_ID, node);
    Preconditions.checkArgument(fieldId > 0, "Invalid field id: %s (must be positive)", fieldId);

    switch (actionType) {
      case IcebergFunction.MASK_ALPHANUM:
        return new MaskAlphanum(fieldId);
      case IcebergFunction.MASK_TO_FIXED_VALUE:
        return new MaskToFixedValue(fieldId);
      case IcebergFunction.REPLACE_WITH_NULL:
        return new ReplaceWithNull(fieldId);
      case IcebergFunction.SHOW_FIRST_4:
        return new ShowFirst4(fieldId);
      case IcebergFunction.SHOW_LAST_4:
        return new ShowLast4(fieldId);
      case IcebergFunction.TRUNCATE_TO_YEAR:
        return new TruncateToYear(fieldId);
      case IcebergFunction.TRUNCATE_TO_MONTH:
        return new TruncateToMonth(fieldId);
      case IcebergFunction.SHA_256_GLOBAL:
        return new Sha256Global(fieldId);
      case IcebergFunction.SHA_256_QUERY_LOCAL:
        return new Sha256QueryLocal(fieldId);
      default:
        // Preserve unknown action types so older clients don't break when the spec evolves.
        // Enforcement code (engine rules) must fail closed on UnknownFunction, since silently
        // skipping an unknown mask would leak unmasked data.
        return new UnknownFunction(fieldId, actionType);
    }
  }
}
