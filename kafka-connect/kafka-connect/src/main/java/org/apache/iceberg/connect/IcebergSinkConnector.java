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
package org.apache.iceberg.connect;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

public class IcebergSinkConnector extends SinkConnector {

  private Map<String, String> props;

  @Override
  public String version() {
    return IcebergSinkConfig.version();
  }

  @Override
  public void start(Map<String, String> connectorProps) {
    this.props = connectorProps;
  }

  @Override
  public Class<? extends Task> taskClass() {
    // FIXME: update this when the connector channel is added
    //  return IcebergSinkTask.class;
    return null;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    String txnSuffix = "-txn-" + UUID.randomUUID() + "-";
    return IntStream.range(0, maxTasks)
        .mapToObj(
            i -> {
              Map<String, String> map = Maps.newHashMap(props);
              map.put(IcebergSinkConfig.INTERNAL_TRANSACTIONAL_SUFFIX_PROP, txnSuffix + i);
              return map;
            })
        .collect(Collectors.toList());
  }

  @Override
  public void stop() {}

  @Override
  public ConfigDef config() {
    return IcebergSinkConfig.CONFIG_DEF;
  }
}
