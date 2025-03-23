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
package org.apache.iceberg.connect.channel;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.connect.Committer;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;

public class MockCommitterImpl implements Committer {
  private boolean isWorkerStarted = false;
  private boolean isCoordinatorStarted = false;
  private List<SinkRecord> records = Lists.newArrayList();

  @Override
  public void start(Catalog catalog, IcebergSinkConfig config, SinkTaskContext context) {
    throw new UnsupportedOperationException(
        "The method start(Catalog, IcebergSinkConfig, SinkTaskContext) is deprecated and will be removed in 2.0.0. "
            + "Use start(Catalog, IcebergSinkConfig, SinkTaskContext, Collection<TopicPartition>) instead.");
  }

  @Override
  public void open(
      Catalog catalog,
      IcebergSinkConfig config,
      SinkTaskContext context,
      Collection<TopicPartition> addedPartitions) {
    isCoordinatorStarted =
        addedPartitions.stream().anyMatch(topicPartition -> topicPartition.partition() == 0);
  }

  @Override
  public void stop() {
    throw new UnsupportedOperationException(
        "The method stop() is deprecated and will be removed in 2.0.0. "
            + "Use stop(Collection<TopicPartition>) instead.");
  }

  @Override
  public void close(Collection<TopicPartition> closedPartitions) {
    isWorkerStarted = false;
    isCoordinatorStarted =
        !(closedPartitions.stream().anyMatch(topicPartition -> topicPartition.partition() == 0));
  }

  @Override
  public void save(Collection<SinkRecord> sinkRecords) {
    if (!isWorkerStarted) {
      isWorkerStarted = true;
    }
    records.addAll(sinkRecords);
  }

  @Override
  public void configure(Map<String, String> config) {}

  public boolean isCoordinatorStarted() {
    return isCoordinatorStarted;
  }
}
