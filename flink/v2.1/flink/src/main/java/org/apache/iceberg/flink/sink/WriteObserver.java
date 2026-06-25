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
package org.apache.iceberg.flink.sink;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.table.data.RowData;

/**
 * Observer that is notified for each record written by {@link IcebergSinkWriter} and produces
 * per-checkpoint metadata that flows through the sink pipeline to the Iceberg snapshot summary.
 *
 * <p>Use cases include per-record watermark extraction, data quality score tracking, or custom
 * metadata that should be attached to each committed Iceberg snapshot.
 *
 * <p>The observer is called on the writer's task thread. {@link #observe(RowData,
 * SinkWriter.Context)} is called for each record. At checkpoint time, {@link #snapshotMetadata()}
 * is called to collect accumulated metadata, which is then carried through the aggregator to the
 * committer and applied as additional Iceberg snapshot properties. The returned metadata map is
 * merged across parallel writer subtasks in the aggregator.
 *
 * <p>Implementations must be {@link Serializable} because the observer travels through the Flink
 * job graph (client → TaskManager) as part of {@link IcebergSink}.
 */
public interface WriteObserver extends Serializable {

  /**
   * Called for each record written by the sink writer.
   *
   * @param element the record being written
   * @param context the sink writer context, providing access to the current Flink watermark
   */
  void observe(RowData element, SinkWriter.Context context);

  /**
   * Called at checkpoint time to collect accumulated metadata for this checkpoint interval.
   *
   * <p>The returned map entries are applied as additional Iceberg snapshot properties alongside the
   * static {@code snapshotProperties} configured on the sink builder. Implementations should reset
   * their internal accumulators after this call so the next checkpoint interval starts fresh.
   *
   * <p>When multiple writer subtasks produce metadata, the aggregator merges them by taking the
   * last value for each key. For watermark-style metadata, implementations should use keys that
   * include a subtask identifier, or the aggregator's merge logic should be considered.
   *
   * @return metadata key-value pairs to include in the snapshot summary, or an empty map
   */
  default Map<String, String> snapshotMetadata() {
    return Collections.emptyMap();
  }

  /**
   * Merges a metadata value when the same key appears from multiple writer subtasks.
   *
   * <p>Called by the aggregator when accumulating metadata from parallel writers. The default
   * returns {@code incoming}, matching last-writer-wins behavior. Override for semantics like
   * {@code Math.min} (watermarks) or {@code Math.max} (high watermarks).
   *
   * @param key the metadata key that appears in both the existing and incoming maps
   * @param existing the previously accumulated value for this key
   * @param incoming the new value from the current writer subtask
   * @return the merged value to keep
   */
  default String mergeValue(String key, String existing, String incoming) {
    return incoming;
  }
}
