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

import java.util.Map;

public final class CreateSnapshotEvent {
  private final String tableName;
  private final String operation;
  private final long snapshotId;
  private final long sequenceNumber;
  private final Map<String, String> summary;

  public CreateSnapshotEvent(
      String tableName,
      String operation,
      long snapshotId,
      long sequenceNumber,
      Map<String, String> summary) {
    this.tableName = tableName;
    this.operation = operation;
    this.snapshotId = snapshotId;
    this.sequenceNumber = sequenceNumber;
    this.summary = summary;
  }

  public String tableName() {
    return tableName;
  }

  public String operation() {
    return operation;
  }

  public long snapshotId() {
    return snapshotId;
  }

  public long sequenceNumber() {
    return sequenceNumber;
  }

  public Map<String, String> summary() {
    return summary;
  }
}
