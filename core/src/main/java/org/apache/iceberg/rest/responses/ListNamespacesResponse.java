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
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.rest.RESTResponse;

public class ListNamespacesResponse implements RESTResponse {

  private List<Namespace> namespaces;
  private String nextPageToken;

  public ListNamespacesResponse() {
    // Required for Jackson deserialization
  }

  private ListNamespacesResponse(List<Namespace> namespaces, String nextPageToken) {
    this.namespaces = namespaces;
    this.nextPageToken = nextPageToken;
    validate();
  }

  @Override
  public void validate() {
    Preconditions.checkArgument(namespaces != null, "Invalid namespace: null");
  }

  public List<Namespace> namespaces() {
    return namespaces != null ? namespaces : ImmutableList.of();
  }

  public String nextPageToken() {
    return nextPageToken;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("namespaces", namespaces())
        .add("next-page-token", nextPageToken())
        .toString();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private final ImmutableList.Builder<Namespace> namespaces = ImmutableList.builder();
    private String nextPageToken;

    private Builder() {}

    public Builder add(Namespace toAdd) {
      Preconditions.checkNotNull(toAdd, "Invalid namespace: null");
      namespaces.add(toAdd);
      return this;
    }

    public Builder addAll(Collection<Namespace> toAdd) {
      Preconditions.checkNotNull(toAdd, "Invalid namespace list: null");
      Preconditions.checkArgument(!toAdd.contains(null), "Invalid namespace: null");
      namespaces.addAll(toAdd);
      return this;
    }

    public Builder nextPageToken(String pageToken) {
      nextPageToken = pageToken;
      return this;
    }

    public ListNamespacesResponse build() {
      return new ListNamespacesResponse(namespaces.build(), nextPageToken);
    }
  }
}
