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
import java.util.List;
import java.util.Map;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergSinkTask extends SinkTask {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergSinkTask.class);

  private IcebergSinkConfig config;
  private Catalog catalog;
  private Committer committer;

  @Override
  public String version() {
    return IcebergSinkConfig.version();
  }

  @Override
  public void start(Map<String, String> props) {
    this.config = new IcebergSinkConfig(props);
  }

  @Override
  public void open(Collection<TopicPartition> partitions) {
    Preconditions.checkArgument(catalog == null, "Catalog already open");
    Preconditions.checkArgument(committer == null, "Committer already open");

    catalog = CatalogUtils.loadCatalog(config);
    committer = CommitterFactory.createCommitter(config);
    committer.start(catalog, config, context);
  }

  @Override
  public void close(Collection<TopicPartition> partitions) {
    close();
    /*
    In Incremental Cooperative Rebalancing, close call happens for only the partition which are revoked from this task.
    It might be possible that this task had [0,1] and due to zero this was co-ordinator,
    and we got the call for close(partition=1), but since we are blindly closing the co-ordinator, this will lead to
    closing of coordinator on this task and since 0 is still retained by this, no other task will be elected as
    coordinator
     */
    if(!context.assignment().isEmpty() && !Sets.newHashSet(context.assignment()).equals(Sets.newHashSet(partitions))) {
      /*
       if this task has at-least 1 partition, calling dummy open() to make sure the co-ordinator is started on this
       in case we closed it on this task and it had partition "0"
       */
      open(List.of());
    }
  }

  private void close() {
    if (committer != null) {
      committer.stop();
      committer = null;
    }

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
  public void put(Collection<SinkRecord> sinkRecords) {
    if (committer != null) {
      committer.save(sinkRecords);
    }
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
    if (committer != null) {
      committer.save(null);
    }
  }

  @Override
  public Map<TopicPartition, OffsetAndMetadata> preCommit(
          Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
    // offset commit is handled by the worker
    return ImmutableMap.of();
  }

  @Override
  public void stop() {
    close();
  }
}