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

public interface Committer {

  /**
   * @deprecated will be removed in 2.0.0. Use {@link #open(Catalog, IcebergSinkConfig,
   *     SinkTaskContext, Collection)} instead.
   */
  @Deprecated
  void start(Catalog catalog, IcebergSinkConfig config, SinkTaskContext context);

  default void open(
      Catalog catalog,
      IcebergSinkConfig config,
      SinkTaskContext context,
      Collection<TopicPartition> addedPartitions) {
    // Default implementation calls the deprecated method. Implementations may override this.
    start(catalog, config, context);
  }

  /**
   * @deprecated will be removed in 2.0.0. Use {@link #close(Collection)} instead.
   */
  @Deprecated
  void stop();

  default void close(Collection<TopicPartition> closedPartitions) {
    // Default implementation calls the deprecated method. Implementations may override this.
    stop();
  }

  void save(Collection<SinkRecord> sinkRecords);
}
