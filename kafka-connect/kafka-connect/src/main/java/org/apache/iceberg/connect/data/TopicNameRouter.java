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
import java.util.regex.Pattern;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * Routes records to Iceberg tables based on the Kafka topic name. Supports namespace prefixing,
 * regex transformations, explicit topic-to-table mappings, and case control.
 *
 * <p>Configuration properties (prefix {@code iceberg.tables.route.topic-name.}):
 *
 * <ul>
 *   <li>{@code table-namespace} — namespace prepended to the derived table name
 *   <li>{@code table-map.<topic>} — explicit topic-to-table override (bypasses all transforms)
 *   <li>{@code regex} — regex applied to the topic name
 *   <li>{@code regex-replacement} — replacement string for the regex (default: {@code $1})
 *   <li>{@code lowercase} — whether to lowercase the derived table name (default: true)
 *   <li>{@code ignore-missing-table} — use NoOpWriter for missing tables instead of throwing
 *       (default: true)
 * </ul>
 */
public class TopicNameRouter implements RecordRouter {

  static final String CONFIG_PREFIX = "iceberg.tables.route.topic-name.";
  static final String TABLE_NAMESPACE_PROP = "table-namespace";
  static final String TABLE_MAP_PREFIX = "table-map.";
  static final String REGEX_PROP = "regex";
  static final String REGEX_REPLACEMENT_PROP = "regex-replacement";
  static final String LOWERCASE_PROP = "lowercase";
  static final String IGNORE_MISSING_TABLE_PROP = "ignore-missing-table";

  private String tableNamespace;
  private Map<String, String> tableMap;
  private Pattern regex;
  private String regexReplacement;
  private boolean lowercase;
  private boolean ignoreMissingTable;

  @Override
  public void configure(Map<String, String> props) {
    Map<String, String> routerProps = PropertyUtil.propertiesWithPrefix(props, CONFIG_PREFIX);

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
  }

  @Override
  public List<RouteTarget> route(SinkRecord record) {
    String topic = record.topic();
    if (topic == null) {
      return ImmutableList.of();
    }

    // explicit mapping takes priority and bypasses all transforms
    String explicitTable = tableMap.get(topic);
    if (explicitTable != null) {
      return ImmutableList.of(RouteTarget.of(explicitTable, ignoreMissingTable));
    }

    String tableName;
    if (regex != null) {
      java.util.regex.Matcher matcher = regex.matcher(topic);
      if (!matcher.matches()) {
        return ImmutableList.of();
      }
      tableName = matcher.replaceAll(regexReplacement);
    } else {
      tableName = topic;
    }

    if (lowercase) {
      tableName = tableName.toLowerCase(Locale.ROOT);
    }

    if (tableNamespace != null) {
      tableName = tableNamespace + "." + tableName;
    }

    return ImmutableList.of(RouteTarget.of(tableName, ignoreMissingTable));
  }
}
