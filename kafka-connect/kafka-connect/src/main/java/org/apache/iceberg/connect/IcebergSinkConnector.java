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

import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.data.PartitionEvolutionUtils;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.Tasks;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class IcebergSinkConnector extends SinkConnector {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergSinkConnector.class);
  private Map<String, String> props;
  private IcebergSinkConfig config;

  @Override
  public String version() {
    return IcebergSinkConfig.version();
  }

  @Override
  public void start(Map<String, String> connectorProps) {
    this.props = connectorProps;
    this.config = new IcebergSinkConfig(this.props);
  }

  @Override
  public Class<? extends Task> taskClass() {
    return IcebergSinkTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    if (config.isPartitionEvolutionSupported()) {
      checkForPartitionEvolution();
    }
    String txnSuffix = generateTransactionSuffix();
    return IntStream.range(0, maxTasks)
            .mapToObj(i -> createTaskConfig(txnSuffix, i))
            .collect(Collectors.toList());
  }

  private String generateTransactionSuffix() {
    return "-txn-" + UUID.randomUUID() + "-";
  }

  private Map<String, String> createTaskConfig(String txnSuffix, int taskNUmber) {
    Map<String, String> taskConfig = Maps.newHashMap(props);
    taskConfig.put(IcebergSinkConfig.INTERNAL_TRANSACTIONAL_SUFFIX_PROP, txnSuffix + taskNUmber);
    return taskConfig;
  }

  private void checkForPartitionEvolution() {
    Catalog catalog = null;
    try {
      catalog = CatalogUtils.loadCatalog(config);
      for(String tableName : config.tables()) {
        Table table = catalog.loadTable(TableIdentifier.parse(tableName));
        Tasks.range(1)
                .retry(IcebergSinkConfig.SCHEMA_UPDATE_RETRIES)
                .run(notUsed -> PartitionEvolutionUtils.checkAndEvolvePartition(table, config));
      }
    } catch (Exception ex) {
      LOG.error("An error occurred while checking for the partition evolution for the job = {}", config.connectorName(), ex);
    } finally {
      if(null != catalog) {
        if(catalog instanceof AutoCloseable) {
          try {
            ((AutoCloseable) catalog).close();
          } catch (Exception e) {
            LOG.warn("An error occurred closing catalog instance, ignoring...", e);
          }
        }
      }
    }
  }

  @Override
  public void stop() {}

  @Override
  public ConfigDef config() {
    return IcebergSinkConfig.CONFIG_DEF;
  }
}