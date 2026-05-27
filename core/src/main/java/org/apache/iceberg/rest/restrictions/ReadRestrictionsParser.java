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
import java.util.List;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionParser;
import org.apache.iceberg.functions.IcebergFunction;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.JsonUtil;

public class ReadRestrictionsParser {

  private ReadRestrictionsParser() {}

  private static final String REQUIRED_ROW_FILTER = "required-row-filter";
  private static final String REQUIRED_COLUMN_PROJECTIONS = "required-column-projections";

  public static String toJson(ReadRestrictions restrictions) {
    return toJson(restrictions, false);
  }

  public static String toJson(ReadRestrictions restrictions, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(restrictions, gen), pretty);
  }

  public static void toJson(ReadRestrictions restrictions, JsonGenerator generator)
      throws IOException {
    Preconditions.checkArgument(restrictions != null, "Invalid read restrictions: null");

    generator.writeStartObject();

    if (restrictions.rowFilter() != null) {
      generator.writeFieldName(REQUIRED_ROW_FILTER);
      ExpressionParser.toJson(restrictions.rowFilter(), generator);
    }

    if (!restrictions.columnProjections().isEmpty()) {
      generator.writeArrayFieldStart(REQUIRED_COLUMN_PROJECTIONS);
      for (IcebergFunction<?, ?> action : restrictions.columnProjections()) {
        ActionParser.toJson(action, generator);
      }
      generator.writeEndArray();
    }

    generator.writeEndObject();
  }

  public static ReadRestrictions fromJson(String json) {
    return JsonUtil.parse(json, ReadRestrictionsParser::fromJson);
  }

  public static ReadRestrictions fromJson(JsonNode node) {
    if (node == null || node.isNull()) {
      return ReadRestrictions.empty();
    }
    Preconditions.checkArgument(
        node.isObject(), "Cannot parse read restrictions from non-object value: %s", node);

    Expression rowFilter = null;
    if (node.hasNonNull(REQUIRED_ROW_FILTER)) {
      rowFilter = ExpressionParser.fromJson(node.get(REQUIRED_ROW_FILTER));
    }

    List<IcebergFunction<?, ?>> actions = Lists.newArrayList();
    if (node.hasNonNull(REQUIRED_COLUMN_PROJECTIONS)) {
      JsonNode array = node.get(REQUIRED_COLUMN_PROJECTIONS);
      Preconditions.checkArgument(
          array.isArray(),
          "Cannot parse required-column-projections: expected array, got: %s",
          array);
      for (JsonNode elem : array) {
        actions.add(ActionParser.fromJson(elem));
      }
    }

    return ReadRestrictions.of(rowFilter, actions);
  }
}
