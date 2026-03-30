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

import java.util.List;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.rest.RESTRequest;

/** Request body for the batch load relations endpoint. */
public class BatchLoadRelationsRequest implements RESTRequest {

  private List<BatchLoadRelationRequestItem> identifiers;

  public BatchLoadRelationsRequest() {
    // Required for Jackson deserialization
  }

  private BatchLoadRelationsRequest(List<BatchLoadRelationRequestItem> identifiers) {
    this.identifiers = identifiers;
  }

  @Override
  public void validate() {
    Preconditions.checkArgument(
        identifiers != null && !identifiers.isEmpty(), "Invalid identifiers: null or empty");
  }

  public List<BatchLoadRelationRequestItem> identifiers() {
    return identifiers;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("identifiers", identifiers).toString();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private final ImmutableList.Builder<BatchLoadRelationRequestItem> identifiers =
        ImmutableList.builder();

    private Builder() {}

    public Builder addIdentifier(BatchLoadRelationRequestItem item) {
      identifiers.add(item);
      return this;
    }

    public Builder addAllIdentifiers(List<BatchLoadRelationRequestItem> items) {
      identifiers.addAll(items);
      return this;
    }

    public BatchLoadRelationsRequest build() {
      BatchLoadRelationsRequest request = new BatchLoadRelationsRequest(identifiers.build());
      request.validate();
      return request;
    }
  }
}
