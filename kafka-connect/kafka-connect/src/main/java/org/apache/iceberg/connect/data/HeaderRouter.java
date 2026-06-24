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

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * Routes records to Iceberg tables based on Kafka record header values.
 *
 * <p>Configuration properties (prefix {@code iceberg.tables.route.header.}):
 *
 * <ul>
 *   <li>{@code name} — header name to extract (required)
 *   <li>{@code table-namespace} — namespace prepended to the derived table name
 *   <li>{@code table-map.<header-value>} — explicit header-value to table overrides
 *   <li>{@code regex} — regex applied to the header value
 *   <li>{@code regex-replacement} — replacement string for regex (default: {@code $1})
 *   <li>{@code lowercase} — lowercase the derived table name (default: true)
 *   <li>{@code ignore-missing-table} — use NoOpWriter for missing tables (default: true)
 *   <li>{@code on-missing-header} — behavior when header is absent: {@code drop}, {@code fail}, or
 *       {@code default:{table}} (default: drop)
 *   <li>{@code multi-value} — when multiple headers with same name: {@code first}, {@code last}, or
 *       {@code all} (default: first)
 * </ul>
 */
public class HeaderRouter implements RecordRouter {

  static final String CONFIG_PREFIX = "iceberg.tables.route.header.";
  static final String NAME_PROP = "name";
  static final String TABLE_NAMESPACE_PROP = "table-namespace";
  static final String TABLE_MAP_PREFIX = "table-map.";
  static final String REGEX_PROP = "regex";
  static final String REGEX_REPLACEMENT_PROP = "regex-replacement";
  static final String LOWERCASE_PROP = "lowercase";
  static final String IGNORE_MISSING_TABLE_PROP = "ignore-missing-table";
  static final String ON_MISSING_HEADER_PROP = "on-missing-header";
  static final String MULTI_VALUE_PROP = "multi-value";

  private String headerName;
  private String tableNamespace;
  private Map<String, String> tableMap;
  private Pattern regex;
  private String regexReplacement;
  private boolean lowercase;
  private boolean ignoreMissingTable;
  private String onMissingHeader;
  private String multiValue;

  @Override
  public void configure(Map<String, String> props) {
    Map<String, String> routerProps = PropertyUtil.propertiesWithPrefix(props, CONFIG_PREFIX);

    this.headerName = routerProps.get(NAME_PROP);
    if (headerName == null || headerName.trim().isEmpty()) {
      throw new ConfigException("Must specify header name in " + CONFIG_PREFIX + NAME_PROP);
    }

    this.tableNamespace = routerProps.get(TABLE_NAMESPACE_PROP);
    this.tableMap = PropertyUtil.propertiesWithPrefix(routerProps, TABLE_MAP_PREFIX);

    String regexStr = routerProps.get(REGEX_PROP);
    this.regex = regexStr != null ? Pattern.compile(regexStr) : null;
    this.regexReplacement = routerProps.getOrDefault(REGEX_REPLACEMENT_PROP, "$1");

    this.lowercase =
        Boolean.parseBoolean(routerProps.getOrDefault(LOWERCASE_PROP, Boolean.TRUE.toString()));
    this.ignoreMissingTable =
        Boolean.parseBoolean(
            routerProps.getOrDefault(IGNORE_MISSING_TABLE_PROP, Boolean.TRUE.toString()));
    this.onMissingHeader = routerProps.getOrDefault(ON_MISSING_HEADER_PROP, "drop");
    this.multiValue = routerProps.getOrDefault(MULTI_VALUE_PROP, "first");
  }

  @Override
  public List<RouteTarget> route(SinkRecord record) {
    if (record.headers() == null || record.headers().isEmpty()) {
      return handleMissingHeader();
    }

    ImmutableList.Builder<String> headerValues = ImmutableList.builder();
    for (Header header : record.headers()) {
      if (headerName.equals(header.key())) {
        String value = decodeHeaderValue(header);
        if (value != null) {
          headerValues.add(value);
        }
      }
    }

    List<String> values = headerValues.build();
    if (values.isEmpty()) {
      return handleMissingHeader();
    }

    List<String> selectedValues;
    switch (multiValue) {
      case "all":
        selectedValues = values;
        break;
      case "last":
        selectedValues = ImmutableList.of(values.get(values.size() - 1));
        break;
      case "first":
      default:
        selectedValues = ImmutableList.of(values.get(0));
        break;
    }

    ImmutableList.Builder<RouteTarget> targets = ImmutableList.builder();
    for (String headerValue : selectedValues) {
      String tableName = resolveTableName(headerValue);
      if (tableName != null) {
        targets.add(RouteTarget.of(tableName, ignoreMissingTable));
      }
    }

    return targets.build();
  }

  private String decodeHeaderValue(Header header) {
    Object value = header.value();
    if (value == null) {
      return null;
    }
    if (value instanceof byte[]) {
      return new String((byte[]) value, StandardCharsets.UTF_8);
    }
    return value.toString();
  }

  private String resolveTableName(String headerValue) {
    // explicit map takes priority and bypasses all transforms
    String explicitTable = tableMap.get(headerValue);
    if (explicitTable != null) {
      return explicitTable;
    }

    String tableName;
    if (regex != null) {
      java.util.regex.Matcher matcher = regex.matcher(headerValue);
      if (!matcher.matches()) {
        return null;
      }
      tableName = matcher.replaceAll(regexReplacement);
    } else {
      tableName = headerValue;
    }

    if (lowercase) {
      tableName = tableName.toLowerCase(Locale.ROOT);
    }

    if (tableNamespace != null) {
      tableName = tableNamespace + "." + tableName;
    }

    return tableName;
  }

  private List<RouteTarget> handleMissingHeader() {
    if ("fail".equals(onMissingHeader)) {
      throw new IllegalArgumentException(
          "Required header '" + headerName + "' not found in record");
    } else if (onMissingHeader.startsWith("default:")) {
      String defaultTable = onMissingHeader.substring("default:".length());
      return ImmutableList.of(RouteTarget.of(defaultTable, ignoreMissingTable));
    }
    // "drop" — return empty list
    return ImmutableList.of();
  }
}
