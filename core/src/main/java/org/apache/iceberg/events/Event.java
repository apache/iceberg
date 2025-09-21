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

import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.rest.operations.Operation;

/** An event to record an operation on {@link org.apache.iceberg.catalog.CatalogObject}. */
public class Event {
  private final String eventId;
  private final String requestId;
  private final Integer eventCount;
  private final Long timestampMs;
  private final String actor;
  private final Operation operation;

  public Event(
      String eventId, String requestId, Integer eventCount, Long timestampMs, Operation operation) {
    this(eventId, requestId, eventCount, timestampMs, "", operation);
  }

  public Event(
      String eventId,
      String requestId,
      Integer eventCount,
      Long timestampMs,
      String actor,
      Operation operation) {
    this.eventId = eventId;
    this.requestId = requestId;
    this.eventCount = eventCount;
    this.timestampMs = timestampMs;
    this.actor = actor;
    this.operation = operation;
  }

  public String eventId() {
    return eventId;
  }

  public String requestId() {
    return requestId;
  }

  public int eventCount() {
    return eventCount;
  }

  public long timestampMs() {
    return timestampMs;
  }

  public String actor() {
    return actor;
  }

  public Operation operation() {
    return operation;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("eventId", eventId)
        .add("requestId", requestId)
        .add("eventCount", eventCount)
        .add("timestampMs", timestampMs)
        .add("actor", actor)
        .add("operation", operation)
        .toString();
  }
}
