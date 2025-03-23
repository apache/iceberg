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
import org.apache.iceberg.connect.channel.MockCommitterImpl;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

public class MockIcebergSinkTask extends SinkTask {

  private IcebergSinkConfig config;
  private Committer committer;

  @Override
  public String version() {
    return "";
  }

  @Override
  public void start(Map<String, String> props) {
    config = new IcebergSinkConfig(props);
    committer = CommitterFactory.createCommitter(config);
  }

  @Override
  public void open(Collection<TopicPartition> partitions) {
    committer.open(null, config, context, partitions);
  }

  @Override
  public void close(Collection<TopicPartition> partitions) {
    committer.close(partitions);
  }

  @Override
  public void put(Collection<SinkRecord> collection) {
    committer.save(collection);
  }

  @Override
  public void stop() {}

  public boolean isCoordinatorRunning() {
    return ((MockCommitterImpl) committer).isCoordinatorStarted();
  }
}
