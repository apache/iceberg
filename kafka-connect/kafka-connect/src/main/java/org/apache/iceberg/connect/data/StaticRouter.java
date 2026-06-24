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
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * Routes records using statically configured table names with optional regex filtering. This
 * preserves the original static routing behavior of the connector.
 */
public class StaticRouter implements RecordRouter {

  private List<String> tables;
  private String routeField;
  private IcebergSinkConfig config;

  @Override
  public void configure(Map<String, String> props) {
    this.config = new IcebergSinkConfig(props);
    this.tables = config.tables() != null ? config.tables() : ImmutableList.of();
    this.routeField = config.tablesRouteField();
  }

  void configure(IcebergSinkConfig sinkConfig) {
    this.config = sinkConfig;
    this.tables = sinkConfig.tables() != null ? sinkConfig.tables() : ImmutableList.of();
    this.routeField = sinkConfig.tablesRouteField();
  }

  @Override
  public List<RouteTarget> route(SinkRecord record) {
    if (routeField == null) {
      return tables.stream().map(RouteTarget::of).collect(Collectors.toList());
    }

    String routeValue = extractRouteValue(record.value(), routeField);
    if (routeValue == null) {
      return ImmutableList.of();
    }

    return tables.stream()
        .filter(
            tableName -> {
              Pattern regex = config.tableConfig(tableName).routeRegex();
              return regex != null && regex.matcher(routeValue).matches();
            })
        .map(RouteTarget::of)
        .collect(Collectors.toList());
  }

  private static String extractRouteValue(Object recordValue, String field) {
    if (recordValue == null) {
      return null;
    }
    Object value = RecordUtils.extractFromRecordValue(recordValue, field);
    return value == null ? null : value.toString();
  }
}
