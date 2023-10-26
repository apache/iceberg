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

import java.util.List;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.RESTResponse;

public class CreateScanResponse implements RESTResponse {

  private String scan;
  private List<String> shards;

  public CreateScanResponse() {}

  private CreateScanResponse(String scan, List<String> shards) {
    this.scan = scan;
    this.shards = shards;
    validate();
  }

  @Override
  public void validate() {
    Preconditions.checkArgument(scan != null, "Invalid scan: null");
    Preconditions.checkArgument(shards != null, "Invalid shards: null");
  }

  public String scan() {
    return scan;
  }

  public List<String> shards() {
    return shards;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("scan", scan).add("shards", shards).toString();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private String scan;

    private List<String> shards;

    private Builder() {}

    public Builder withScan(String newScan) {
      Preconditions.checkNotNull(newScan, "Invalid scan: null");
      this.scan = newScan;
      return this;
    }

    public Builder withShards(List<String> newShards) {
      Preconditions.checkNotNull(newShards, "Invalid shards: null");
      this.shards = newShards;
      return this;
    }

    public CreateScanResponse build() {
      return new CreateScanResponse(scan, shards);
    }
  }
}
