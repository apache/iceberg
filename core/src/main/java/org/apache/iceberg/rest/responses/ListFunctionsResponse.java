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
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.rest.RESTResponse;

/** A list of function names for a given namespace. */
public class ListFunctionsResponse implements RESTResponse {

  private List<String> names;
  private String nextPageToken;

  public ListFunctionsResponse() {
    // Required for Jackson deserialization
  }

  private ListFunctionsResponse(List<String> names, String nextPageToken) {
    this.names = names;
    this.nextPageToken = nextPageToken;
    validate();
  }

  @Override
  public void validate() {
    Preconditions.checkArgument(names != null, "Invalid function name list: null");
  }

  public List<String> names() {
    return names != null ? names : ImmutableList.of();
  }

  public String nextPageToken() {
    return nextPageToken;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("names", names)
        .add("next-page-token", nextPageToken())
        .toString();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private final ImmutableList.Builder<String> names = ImmutableList.builder();
    private String nextPageToken;

    private Builder() {}

    public Builder add(String name) {
      Preconditions.checkNotNull(name, "Invalid function name: null");
      names.add(name);
      return this;
    }

    public Builder addAll(List<String> toAdd) {
      Preconditions.checkNotNull(toAdd, "Invalid function name list: null");
      Preconditions.checkArgument(!toAdd.contains(null), "Invalid function name: null");
      names.addAll(toAdd);
      return this;
    }

    public Builder nextPageToken(String pageToken) {
      nextPageToken = pageToken;
      return this;
    }

    public ListFunctionsResponse build() {
      return new ListFunctionsResponse(names.build(), nextPageToken);
    }
  }
}
