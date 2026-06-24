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
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * Routes records dynamically using a field value as the target table name. This preserves the
 * original dynamic routing behavior of the connector.
 */
public class DynamicRouter implements RecordRouter {

  private String routeField;

  @Override
  public void configure(Map<String, String> props) {
    IcebergSinkConfig config = new IcebergSinkConfig(props);
    this.routeField = config.tablesRouteField();
    Preconditions.checkNotNull(routeField, "Route field cannot be null with dynamic routing");
  }

  void configure(IcebergSinkConfig config) {
    this.routeField = config.tablesRouteField();
    Preconditions.checkNotNull(routeField, "Route field cannot be null with dynamic routing");
  }

  @Override
  public List<RouteTarget> route(SinkRecord record) {
    String routeValue = extractRouteValue(record.value(), routeField);
    if (routeValue == null) {
      return ImmutableList.of();
    }
    String tableName = routeValue.toLowerCase(Locale.ROOT);
    return ImmutableList.of(RouteTarget.of(tableName, true));
  }

  private static String extractRouteValue(Object recordValue, String field) {
    if (recordValue == null) {
      return null;
    }
    Object value = RecordUtils.extractFromRecordValue(recordValue, field);
    return value == null ? null : value.toString();
  }
}
