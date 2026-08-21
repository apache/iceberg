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
package org.apache.iceberg.metrics;

import org.immutables.value.Value;

@Value.Immutable
public abstract class RemoveSnapshotsReport implements MetricsReport {
  public abstract long dataFilesCount();

  public abstract long positionDeleteFilesCount();

  public abstract long equalityDeleteFilesCount();

  public abstract long manifestsCount();

  public abstract long manifestListsCount();

  public abstract long statisticsFilesCount();

  public static RemoveSnapshotsReport of(
      long dataFilesCount,
      long positionDeleteFilesCount,
      long equalityDeleteFilesCount,
      long manifestsCount,
      long manifestListsCount,
      long statisticsFilesCount) {
    return ImmutableRemoveSnapshotsReport.builder()
        .dataFilesCount(dataFilesCount)
        .positionDeleteFilesCount(positionDeleteFilesCount)
        .equalityDeleteFilesCount(equalityDeleteFilesCount)
        .manifestsCount(manifestsCount)
        .manifestListsCount(manifestListsCount)
        .statisticsFilesCount(statisticsFilesCount)
        .build();
  }
}
