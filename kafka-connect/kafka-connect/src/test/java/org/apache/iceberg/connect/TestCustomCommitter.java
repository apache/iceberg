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

import java.util.Collection;
import org.apache.iceberg.catalog.Catalog;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Example custom Committer implementation for testing and demonstration purposes.
 *
 * <p>This is a simple example showing how to implement a custom Committer. In practice, custom
 * committers might:
 *
 * <ul>
 *   <li>Implement different commit strategies (e.g., time-based, size-based)
 *   <li>Use alternative coordination mechanisms (e.g., external consensus systems)
 *   <li>Add custom monitoring and metrics
 *   <li>Implement specialized error handling and retry logic
 * </ul>
 *
 * <p>To use this custom committer, configure:
 *
 * <pre>
 * iceberg.committer.class=org.apache.iceberg.connect.TestCustomCommitter
 * iceberg.committer.custom-property=value
 * </pre>
 */
public class TestCustomCommitter implements Committer {

  private static final Logger LOG = LoggerFactory.getLogger(TestCustomCommitter.class);

  private Catalog catalog;
  private IcebergSinkConfig config;
  private SinkTaskContext context;
  private boolean started = false;

  /** Public no-arg constructor required for dynamic instantiation. */
  public TestCustomCommitter() {
    LOG.info("TestCustomCommitter instantiated");
  }

  @Override
  public void onTaskStarted(IcebergSinkConfig config, SinkTaskContext context) {
    this.catalog = CatalogUtils.loadCatalog(config);
    this.config = config;
    this.context = context;
    this.started = true;
  }

  @Override
  public void onPartitionsAdded(Collection<TopicPartition> addedPartitions) {
    LOG.info(
        "TestCustomCommitter.onPartitionsAdded() called - catalog={}, config={}, addedPartitions={}",
        catalog.name(),
        config.connectorName(),
        addedPartitions);
  }

  @Override
  public void onPartitionsRemoved(Collection<TopicPartition> closedPartitions) {
    LOG.info(
        "TestCustomCommitter.onPartitionsRemoved() called - closedPartitions={}", closedPartitions);
  }

  @Override
  public void onTaskStopped() {
    this.started = false;
    closeCatalog();
  }

  private void closeCatalog() {
    if (catalog != null) {
      if (catalog instanceof AutoCloseable) {
        try {
          ((AutoCloseable) catalog).close();
        } catch (Exception e) {
          LOG.warn("An error occurred closing catalog instance, ignoring...", e);
        }
      }
      catalog = null;
    }
  }

  @Override
  public void save(Collection<SinkRecord> sinkRecords) {
    LOG.info("TestCustomCommitter.save() called - record count={}", sinkRecords.size());

    if (!started) {
      LOG.warn("TestCustomCommitter.save() called but committer not started");
      return;
    }

    // In a real implementation, you would:
    // 1. Write the records to data files
    // 2. Coordinate with other tasks (if needed)
    // 3. Commit the data files to the Iceberg table
    // 4. Handle errors and retries

    for (SinkRecord record : sinkRecords) {
      LOG.debug(
          "Processing record: topic={}, partition={}, offset={}",
          record.topic(),
          record.kafkaPartition(),
          record.kafkaOffset());
    }
  }

  // Getter methods for testing
  public boolean isStarted() {
    return started;
  }

  public Catalog getCatalog() {
    return catalog;
  }

  public IcebergSinkConfig getConfig() {
    return config;
  }

  public SinkTaskContext getContext() {
    return context;
  }
}
