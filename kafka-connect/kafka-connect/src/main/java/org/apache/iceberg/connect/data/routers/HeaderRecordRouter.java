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

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.connect.RecordRouter;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;

public class HeaderRecordRouter implements RecordRouter {
  private final List<String> keysForHeaderRouting = Lists.newArrayList();
  private final Map<String, String> headerValueToTableMap = Maps.newHashMap();

  @Override
  public List<String> tables(SinkRecord record) {
    List<String> tables = Lists.newArrayList();
    for (Header header : record.headers()) {
      String keyFromHeader = header.key();
      if (keysForHeaderRouting.contains(keyFromHeader)) {
        String valueFromHeader = new String((byte[]) header.value(), StandardCharsets.UTF_8);
        if (headerValueToTableMap.isEmpty()) {
          tables.add(valueFromHeader);
        }
        if (headerValueToTableMap.containsKey(valueFromHeader)) {
          tables.add(headerValueToTableMap.get(valueFromHeader));
        }
      }
    }
    return tables;
  }

  @Override
  public void configure(Map<String, String> props) {
    if (!props.containsKey(RouterConstants.HEADER_ROUTE_KEYS)) {
      throw new ConfigException("Header keys are required in header route mode");
    }
    keysForHeaderRouting.addAll(
        Splitter.on(",").splitToList(props.get(RouterConstants.HEADER_ROUTE_KEYS)));
    headerValueToTableMap.putAll(tableMapping(props));
  }

  private Map<String, String> tableMapping(Map<String, String> props) {
    return Arrays.stream(props.getOrDefault(RouterConstants.TABLE_MAPPING, "").split(","))
        .filter(s -> !s.isEmpty())
        .map(s -> s.split(":", 2))
        .filter(ss -> ss.length == 2)
        .collect(Collectors.toMap(ss -> ss[0].trim(), ss -> ss[1].trim()));
  }
}
