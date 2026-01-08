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
package org.apache.iceberg.view;

import java.util.List;

/**
 * Represents the refresh state metadata for a materialized view.
 *
 * <p>Refresh state captures which version of the materialized view was used, the state of all
 * source dependencies (tables, views, or other materialized views), and when the refresh started.
 * This metadata is stored in the snapshot summary of the storage table to enable freshness
 * evaluation.
 */
public interface RefreshState {

  /**
   * The ID of the materialized view version that was used to generate this refresh.
   *
   * @return the view version ID
   */
  int viewVersionId();

  /**
   * The list of source states representing the versions of all source dependencies at refresh time.
   *
   * <p>For diamond dependencies (same source referenced multiple times), the list must contain the
   * entry with the oldest snapshot-id or version-id for that source, as per the spec.
   *
   * @return list of source states, may be empty if dependencies cannot be determined
   */
  List<SourceState> sourceStates();

  /**
   * The timestamp in milliseconds (from epoch) when the refresh operation started.
   *
   * <p>This timestamp is used for coarse-grained freshness evaluation based on max-staleness-ms.
   *
   * @return refresh start timestamp in milliseconds
   */
  long refreshStartTimestampMs();
}
