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
package org.apache.iceberg.events.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public class EventsResponse {

  @JsonProperty("next-page-token")
  private String nextPageToken;

  @JsonProperty("highest-processed-timestamp-ms")
  private Long highestProcessedTimestampMs;

  @JsonProperty("highest-processed-sequence")
  private Long highestProcessedSequence;

  @JsonProperty("events")
  private List<Event> events;

  // Getters and setters
  public String getNextPageToken() {
    return nextPageToken;
  }

  public void setNextPageToken(String nextPageToken) {
    this.nextPageToken = nextPageToken;
  }

  public Long getHighestProcessedTimestampMs() {
    return highestProcessedTimestampMs;
  }

  public void setHighestProcessedTimestampMs(Long highestProcessedTimestampMs) {
    this.highestProcessedTimestampMs = highestProcessedTimestampMs;
  }

  public Long getHighestProcessedSequence() {
    return highestProcessedSequence;
  }

  public void setHighestProcessedSequence(Long highestProcessedSequence) {
    this.highestProcessedSequence = highestProcessedSequence;
  }

  public List<Event> getEvents() {
    return events;
  }

  public void setEvents(List<Event> events) {
    this.events = events;
  }
}
