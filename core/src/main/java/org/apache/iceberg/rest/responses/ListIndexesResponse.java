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

import java.util.Collection;
import java.util.List;
import org.apache.iceberg.catalog.IndexIdentifier;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.rest.RESTResponse;

/** A list of index identifiers for a given table. */
public class ListIndexesResponse implements RESTResponse {

  private List<IndexIdentifier> identifiers;
  private String nextPageToken;

  public ListIndexesResponse() {
    // Required for Jackson deserialization
  }

  private ListIndexesResponse(List<IndexIdentifier> identifiers, String nextPageToken) {
    this.identifiers = identifiers;
    this.nextPageToken = nextPageToken;
    validate();
  }

  @Override
  public void validate() {
    Preconditions.checkArgument(identifiers != null, "Invalid identifier list: null");
  }

  public List<IndexIdentifier> identifiers() {
    return identifiers != null ? identifiers : ImmutableList.of();
  }

  public String nextPageToken() {
    return nextPageToken;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("identifiers", identifiers)
        .add("next-page-token", nextPageToken())
        .toString();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private final ImmutableList.Builder<IndexIdentifier> identifiers = ImmutableList.builder();
    private String nextPageToken;

    private Builder() {}

    public Builder add(IndexIdentifier toAdd) {
      Preconditions.checkNotNull(toAdd, "Invalid index identifier: null");
      identifiers.add(toAdd);
      return this;
    }

    public Builder addAll(Collection<IndexIdentifier> toAdd) {
      Preconditions.checkNotNull(toAdd, "Invalid index identifier list: null");
      Preconditions.checkArgument(!toAdd.contains(null), "Invalid index identifier: null");
      identifiers.addAll(toAdd);
      return this;
    }

    public Builder nextPageToken(String pageToken) {
      nextPageToken = pageToken;
      return this;
    }

    public ListIndexesResponse build() {
      return new ListIndexesResponse(identifiers.build(), nextPageToken);
    }
  }
}
