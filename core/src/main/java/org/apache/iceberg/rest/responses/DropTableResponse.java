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

package org.apache.iceberg.rest.responses;

import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Represents a REST response to drop a table.
 */
public class DropTableResponse {

  // For Jackson to properly fail when deserializing, this needs to be boxed.
  // Otherwise, the boolean is parsed according to "loose" javascript JSON rules.
  private Boolean dropped;

  public DropTableResponse() {
    // Required for Jackson deserialization
  }

  private DropTableResponse(boolean dropped) {
    this.dropped = dropped;
    validate();
  }

  DropTableResponse validate() {
    Preconditions.checkArgument(dropped != null, "Invalid response, missing field: dropped");
    return this;
  }

  public boolean isDropped() {
    return dropped;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("dropped", dropped)
        .toString();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private Boolean dropped;

    private Builder() {
    }

    public Builder dropped(boolean isDropped) {
      this.dropped = isDropped;
      return this;
    }

    public DropTableResponse build() {
      Preconditions.checkArgument(dropped != null, "Invalid response, missing field: dropped");
      return new DropTableResponse(dropped);
    }
  }
}

