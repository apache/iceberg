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
package org.apache.iceberg.connect.data;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * Routes records to Iceberg tables based on multiple record fields combined via a template.
 *
 * <p>Configuration properties (prefix {@code iceberg.tables.route.composite.}):
 *
 * <ul>
 *   <li>{@code fields} — comma-separated list of field paths (dot notation for nesting)
 *   <li>{@code table-template} — template for building the table name ({@code ${0}}, {@code ${1}}
 *       reference fields by index)
 *   <li>{@code table-namespace} — namespace prepended to the derived table name
 *   <li>{@code lowercase} — whether to lowercase the derived table name (default: true)
 *   <li>{@code ignore-missing-table} — use NoOpWriter for missing tables (default: true)
 *   <li>{@code null-handling} — behavior when a field is null: {@code drop}, {@code literal}, or
 *       {@code default:<value>} (default: drop)
 * </ul>
 */
public class CompositeFieldRouter implements RecordRouter {

  static final String CONFIG_PREFIX = "iceberg.tables.route.composite.";
  static final String FIELDS_PROP = "fields";
  static final String TABLE_TEMPLATE_PROP = "table-template";
  static final String TABLE_NAMESPACE_PROP = "table-namespace";
  static final String LOWERCASE_PROP = "lowercase";
  static final String IGNORE_MISSING_TABLE_PROP = "ignore-missing-table";
  static final String NULL_HANDLING_PROP = "null-handling";

  private static final Pattern TEMPLATE_VAR = Pattern.compile("\\$\\{(\\d+)}");

  private List<String> fields;
  private String tableTemplate;
  private String tableNamespace;
  private boolean lowercase;
  private boolean ignoreMissingTable;
  private String nullHandling;

  @Override
  public void configure(Map<String, String> props) {
    Map<String, String> routerProps = PropertyUtil.propertiesWithPrefix(props, CONFIG_PREFIX);

    String fieldsStr = routerProps.get(FIELDS_PROP);
    if (fieldsStr == null || fieldsStr.trim().isEmpty()) {
      throw new ConfigException(
          "Must specify at least one field in " + CONFIG_PREFIX + FIELDS_PROP);
    }
    this.fields = Splitter.on(',').trimResults().splitToList(fieldsStr);

    this.tableTemplate = routerProps.getOrDefault(TABLE_TEMPLATE_PROP, buildDefaultTemplate());
    this.tableNamespace = routerProps.get(TABLE_NAMESPACE_PROP);
    this.lowercase =
        Boolean.parseBoolean(routerProps.getOrDefault(LOWERCASE_PROP, Boolean.TRUE.toString()));
    this.ignoreMissingTable =
        Boolean.parseBoolean(
            routerProps.getOrDefault(IGNORE_MISSING_TABLE_PROP, Boolean.TRUE.toString()));
    this.nullHandling = routerProps.getOrDefault(NULL_HANDLING_PROP, "drop");

    validateTemplate();
  }

  private String buildDefaultTemplate() {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < fields.size(); i++) {
      if (i > 0) {
        sb.append(".");
      }
      sb.append("${").append(i).append("}");
    }
    return sb.toString();
  }

  private void validateTemplate() {
    Matcher matcher = TEMPLATE_VAR.matcher(tableTemplate);
    while (matcher.find()) {
      int index = Integer.parseInt(matcher.group(1));
      if (index >= fields.size()) {
        throw new ConfigException(
            String.format(
                Locale.ROOT,
                "Template variable ${%d} references field index %d, but only %d fields configured",
                index,
                index,
                fields.size()));
      }
    }
  }

  @Override
  public List<RouteTarget> route(SinkRecord record) {
    if (record.value() == null) {
      return ImmutableList.of();
    }

    String[] values = new String[fields.size()];
    for (int i = 0; i < fields.size(); i++) {
      Object extracted = RecordUtils.extractFromRecordValue(record.value(), fields.get(i));
      if (extracted == null) {
        String resolved = resolveNull();
        if (resolved == null) {
          return ImmutableList.of();
        }
        values[i] = resolved;
      } else {
        values[i] = extracted.toString();
      }
    }

    String tableName = applyTemplate(values);

    if (lowercase) {
      tableName = tableName.toLowerCase(Locale.ROOT);
    }

    if (tableNamespace != null) {
      tableName = tableNamespace + "." + tableName;
    }

    return ImmutableList.of(RouteTarget.of(tableName, ignoreMissingTable));
  }

  private String applyTemplate(String[] values) {
    Matcher matcher = TEMPLATE_VAR.matcher(tableTemplate);
    StringBuilder sb = new StringBuilder();
    while (matcher.find()) {
      int index = Integer.parseInt(matcher.group(1));
      matcher.appendReplacement(sb, Matcher.quoteReplacement(values[index]));
    }
    matcher.appendTail(sb);
    return sb.toString();
  }

  private String resolveNull() {
    if ("drop".equals(nullHandling)) {
      return null;
    } else if ("literal".equals(nullHandling)) {
      return "null";
    } else if (nullHandling.startsWith("default:")) {
      return nullHandling.substring("default:".length());
    }
    return null;
  }
}
