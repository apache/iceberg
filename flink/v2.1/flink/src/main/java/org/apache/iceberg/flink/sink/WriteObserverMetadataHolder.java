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

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Thread-local holder for passing {@link WriteObserver} metadata through the WriteResult
 * serialization boundary.
 *
 * <p>On the writer side: {@link IcebergSinkWriter#prepareCommit()} stores metadata here, then
 * {@link WriteResultSerializer#serialize} reads and clears it. Both run on the writer's task
 * thread.
 *
 * <p>On the aggregator side: {@link WriteResultSerializer#deserialize} stores metadata here, then
 * {@link IcebergWriteAggregator#processElement} reads and clears it. Both run on the aggregator's
 * task thread.
 *
 * <p>This is safe because Flink processes elements sequentially on each task thread -- serialize
 * and prepareCommit share one thread, deserialize and processElement share another.
 */
final class WriteObserverMetadataHolder {
  private static final ThreadLocal<Map<String, String>> METADATA = new ThreadLocal<>();

  private WriteObserverMetadataHolder() {}

  static void set(@Nullable Map<String, String> metadata) {
    METADATA.set(metadata);
  }

  @Nullable
  static Map<String, String> getAndClear() {
    Map<String, String> metadata = METADATA.get();
    METADATA.remove();
    return metadata;
  }
}
