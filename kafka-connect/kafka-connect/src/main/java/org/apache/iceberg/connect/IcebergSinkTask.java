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
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergSinkTask extends SinkTask {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergSinkTask.class);

  private Committer committer;
  private String identifier;

  @Override
  public String version() {
    return IcebergSinkConfig.version();
  }

  @Override
  public void start(Map<String, String> props) {
    IcebergSinkConfig config = new IcebergSinkConfig(props);
    this.committer = CommitterFactory.createCommitter(config);
    this.committer.onTaskStarted(config, context);
    this.identifier = config.connectorName() + "-" + config.taskId();
  }

  @Override
  public void open(Collection<TopicPartition> partitions) {
    committer.onPartitionsAdded(partitions);
  }

  @Override
  public void close(Collection<TopicPartition> partitions) {
    committer.onPartitionsRemoved(partitions);
  }

  private void close() {
    if (committer != null) {
      committer.onTaskStopped();
      committer = null;
    }
  }

  @Override
  public void put(Collection<SinkRecord> sinkRecords) {
    if (committer != null) {
      committer.save(sinkRecords);
      return;
    }
    LOG.info(
        "Task {} received {} records without committer initialization",
        identifier,
        sinkRecords.size());
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
