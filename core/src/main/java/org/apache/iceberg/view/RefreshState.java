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
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Captures the state of source tables and views at the time of a materialized view refresh
 * operation. Stored as a JSON-encoded string in the storage table's snapshot summary under the
 * {@code refresh-state} key.
 */
public class RefreshState {
  public static final String REFRESH_STATE_SUMMARY_KEY = "refresh-state";

  private final int viewVersionId;
  private final List<SourceState> sourceStates;
  private final long refreshStartTimestampMs;

  public RefreshState(
      int viewVersionId, List<SourceState> sourceStates, long refreshStartTimestampMs) {
    Preconditions.checkArgument(sourceStates != null, "Source states list is required");
    this.viewVersionId = viewVersionId;
    this.sourceStates = sourceStates;
    this.refreshStartTimestampMs = refreshStartTimestampMs;
  }

  public int viewVersionId() {
    return viewVersionId;
  }

  public List<SourceState> sourceStates() {
    return sourceStates;
  }

  public long refreshStartTimestampMs() {
    return refreshStartTimestampMs;
  }
}
