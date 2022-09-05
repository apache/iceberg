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
package org.apache.iceberg.events;

import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;

/** Event sent to listeners when an incremental table scan is planned. */
public final class IncrementalScanEvent {
  private final String tableName;
  private final long fromSnapshotId;
  private final long toSnapshotId;
  private final Expression filter;
  private final Schema projection;
  private final boolean fromSnapshotInclusive;

  public IncrementalScanEvent(
      String tableName,
      long fromSnapshotId,
      long toSnapshotId,
      Expression filter,
      Schema projection,
      boolean fromSnapshotInclusive) {
    this.tableName = tableName;
    this.fromSnapshotId = fromSnapshotId;
    this.toSnapshotId = toSnapshotId;
    this.filter = filter;
    this.projection = projection;
    this.fromSnapshotInclusive = fromSnapshotInclusive;
  }

  public String tableName() {
    return tableName;
  }

  public long fromSnapshotId() {
    return fromSnapshotId;
  }

  public long toSnapshotId() {
    return toSnapshotId;
  }

  public Expression filter() {
    return filter;
  }

  public Schema projection() {
    return projection;
  }

  public boolean isFromSnapshotInclusive() {
    return fromSnapshotInclusive;
  }
}
