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
package org.apache.iceberg.rest.requests;

import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.RESTRequest;

public class GetScanTasksRequest implements RESTRequest {

  private String scanID;

  private String shard;

  public GetScanTasksRequest() {}

  public GetScanTasksRequest(String scanID, String shard) {
    this.scanID = scanID;
    this.shard = shard;
    validate();
  }

  @Override
  public void validate() {
    Preconditions.checkArgument(scanID != null, "Invalid scanID: null");
    Preconditions.checkArgument(shard != null, "Invalid shard: null");
  }

  public String scanID() {
    return scanID;
  }

  public String shard() {
    return shard;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("scanID", scanID).add("shard", shard).toString();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private String scanID;

    private String shard;

    private Builder() {}

    public Builder withScanID(String newScanID) {
      Preconditions.checkNotNull(newScanID, "Invalid selections: null");
      this.scanID = newScanID;
      return this;
    }

    public Builder withShard(String newShard) {
      this.shard = newShard;
      return this;
    }

    public GetScanTasksRequest build() {
      return new GetScanTasksRequest(scanID, shard);
    }
  }
}
