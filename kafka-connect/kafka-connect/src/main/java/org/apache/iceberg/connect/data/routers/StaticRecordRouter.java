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
package org.apache.iceberg.connect.data.routers;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.RecordRouter;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.kafka.connect.sink.SinkRecord;

public class StaticRecordRouter implements RecordRouter {

  private String routeField;
  private final List<String> tables = Lists.newArrayList();
  private final Map<String, String> tablesRouteRegex = Maps.newHashMap();

  @Override
  public List<String> tables(SinkRecord record) {
    if (routeField == null) {
      return tables;
    } else {
      List<String> matchedTables = Lists.newArrayList();
      String routeValue = RouterUtils.extractRouteValue(record.value(), routeField);
      if (routeValue != null) {
        tablesRouteRegex.forEach(
            (table, routeRegex) -> {
              if (!routeRegex.isBlank()
                  && Pattern.compile(routeRegex).matcher(routeValue).matches()) {
                matchedTables.add(table);
              }
            });
      }
      return matchedTables;
    }
  }

  @Override
  public void configure(Map<String, String> props) {
    routeField = props.get(RouterConstants.TABLES_ROUTE_FIELD_PROP);
    tables.addAll(Splitter.on(',').splitToList(props.get(RouterConstants.TABLES_PROP)));
    if (null == routeField || routeField.isBlank()) {
      return;
    }
    for (String table : tables) {
      String tableName = TableIdentifier.parse(table).name();
      if (props.containsKey(
          RouterConstants.TABLE_PROP_PREFIX + tableName + "." + RouterConstants.ROUTE_REGEX)) {
        tablesRouteRegex.put(
            table,
            props.get(
                RouterConstants.TABLE_PROP_PREFIX + tableName + "." + RouterConstants.ROUTE_REGEX));
      }
    }
  }
}
