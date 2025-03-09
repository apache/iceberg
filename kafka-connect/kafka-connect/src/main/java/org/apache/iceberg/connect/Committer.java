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
import org.apache.iceberg.catalog.Catalog;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;

/**
 * The Committer interface defines methods for handling the commit process
 * in the Iceberg Sink Connector. It provides operations for starting, stopping,
 * and saving sink records in relation to Kafka topic partitions.
 */
public interface Committer {

  /**
   * Starts the committer with the provided catalog, configuration, and task context.
   * This method is deprecated in favor of the method that accepts added partitions.
   *
   * @param catalog The Iceberg catalog for interacting with tables.
   * @param config The configuration for the Iceberg Sink.
   * @param context The context of the Kafka Connect sink task.
   */
  @Deprecated
  void start(Catalog catalog, IcebergSinkConfig config, SinkTaskContext context);

  /**
   * Starts the committer with the provided catalog, configuration, task context,
   * and the collection of added Kafka topic partitions.
   *
   * This method provides more flexibility by allowing the specification of
   * partitions that were added.
   *
   * @param catalog The Iceberg catalog for interacting with tables.
   * @param config The configuration for the Iceberg Sink.
   * @param context The context of the Kafka Connect sink task.
   * @param addedPartitions The Kafka topic partitions that were added.
   */
  default void start(Catalog catalog, IcebergSinkConfig config, SinkTaskContext context,
                     Collection<TopicPartition> addedPartitions) {
    // Default implementation calls the deprecated method. Implementations may override this.
    start(catalog, config, context);
  }

  /**
   * Stops the committer. This method is deprecated in favor of the method
   * that accepts closed partitions.
   */
  @Deprecated
  void stop();

  /**
   * Stops the committer, handling the collection of closed Kafka topic partitions.
   * This allows the committer to clean up resources related to the partitions
   * that have been closed.
   *
   * @param closedPartitions The Kafka topic partitions that were closed.
   */
  default void stop(Collection<TopicPartition> closedPartitions) {
    // Default implementation calls the deprecated method. Implementations may override this.
    stop();
  }

  /**
   * Saves the provided sink records. This method is called when the records need
   * to be persisted as part of the commit process.
   *
   * @param sinkRecords A collection of SinkRecord instances to be saved.
   */
  void save(Collection<SinkRecord> sinkRecords);

  default Committer configure(Map<String, String> config) {
    return this;
  }
}
