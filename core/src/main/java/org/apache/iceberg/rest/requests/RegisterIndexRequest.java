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

/** A REST request to register an existing index by its metadata file location. */
public class RegisterIndexRequest implements RESTRequest {

  private String name;
  private String metadataLocation;

  public RegisterIndexRequest() {
    // Required for Jackson deserialization
  }

  private RegisterIndexRequest(String name, String metadataLocation) {
    this.name = name;
    this.metadataLocation = metadataLocation;
    validate();
  }

  @Override
  public void validate() {
    Preconditions.checkArgument(
        name != null && !name.isEmpty(), "Invalid index name: null or empty");
    Preconditions.checkArgument(
        metadataLocation != null && !metadataLocation.isEmpty(),
        "Invalid metadata location: null or empty");
  }

  public String name() {
    return name;
  }

  public String metadataLocation() {
    return metadataLocation;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("name", name)
        .add("metadataLocation", metadataLocation)
        .toString();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private String name;
    private String metadataLocation;

    private Builder() {}

    public Builder withName(String indexName) {
      this.name = indexName;
      return this;
    }

    public Builder withMetadataLocation(String location) {
      this.metadataLocation = location;
      return this;
    }

    public RegisterIndexRequest build() {
      return new RegisterIndexRequest(name, metadataLocation);
    }
  }
}
