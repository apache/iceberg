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
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.rest.RESTResponse;

/** A list of table identifiers in a given namespace. */
public class ListTablesResponse implements RESTResponse {

  private List<TableIdentifier> identifiers;

  public ListTablesResponse() {
    // Required for Jackson deserialization
  }

  private ListTablesResponse(List<TableIdentifier> identifiers) {
    this.identifiers = identifiers;
    validate();
  }

  @Override
  public void validate() {
    Preconditions.checkArgument(identifiers != null, "Invalid identifier list: null");
  }

  public List<TableIdentifier> identifiers() {
    return identifiers != null ? identifiers : ImmutableList.of();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("identifiers", identifiers).toString();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private final ImmutableList.Builder<TableIdentifier> identifiers = ImmutableList.builder();

    private Builder() {}

    public Builder add(TableIdentifier toAdd) {
      Preconditions.checkNotNull(toAdd, "Invalid table identifier: null");
      identifiers.add(toAdd);
      return this;
    }

    public Builder addAll(Collection<TableIdentifier> toAdd) {
      Preconditions.checkNotNull(toAdd, "Invalid table identifier list: null");
      Preconditions.checkArgument(!toAdd.contains(null), "Invalid table identifier: null");
      identifiers.addAll(toAdd);
      return this;
    }

    public ListTablesResponse build() {
      return new ListTablesResponse(identifiers.build());
    }
  }
}
