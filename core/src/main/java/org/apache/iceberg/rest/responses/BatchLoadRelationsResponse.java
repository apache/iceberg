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
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.rest.RESTResponse;

/** Response body for the batch load relations endpoint. */
public class BatchLoadRelationsResponse implements RESTResponse {

  private List<BatchLoadRelationResultItem> results;
  private List<TableIdentifier> unprocessedIdentifiers;

  public BatchLoadRelationsResponse() {
    // Required for Jackson deserialization
  }

  private BatchLoadRelationsResponse(
      List<BatchLoadRelationResultItem> results, List<TableIdentifier> unprocessedIdentifiers) {
    this.results = results;
    this.unprocessedIdentifiers = unprocessedIdentifiers;
  }

  @Override
  public void validate() {
    Preconditions.checkNotNull(results, "Invalid results: null");
  }

  public List<BatchLoadRelationResultItem> results() {
    return results;
  }

  public List<TableIdentifier> unprocessedIdentifiers() {
    return unprocessedIdentifiers != null ? unprocessedIdentifiers : ImmutableList.of();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("results", results)
        .add("unprocessedIdentifiers", unprocessedIdentifiers)
        .toString();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private final ImmutableList.Builder<BatchLoadRelationResultItem> results =
        ImmutableList.builder();
    private ImmutableList.Builder<TableIdentifier> unprocessedIdentifiers;

    private Builder() {}

    public Builder addResult(BatchLoadRelationResultItem item) {
      results.add(item);
      return this;
    }

    public Builder addAllResults(List<BatchLoadRelationResultItem> items) {
      results.addAll(items);
      return this;
    }

    public Builder addUnprocessedIdentifier(TableIdentifier ident) {
      if (unprocessedIdentifiers == null) {
        unprocessedIdentifiers = ImmutableList.builder();
      }

      unprocessedIdentifiers.add(ident);
      return this;
    }

    public Builder addAllUnprocessedIdentifiers(List<TableIdentifier> idents) {
      if (unprocessedIdentifiers == null) {
        unprocessedIdentifiers = ImmutableList.builder();
      }

      unprocessedIdentifiers.addAll(idents);
      return this;
    }

    public BatchLoadRelationsResponse build() {
      BatchLoadRelationsResponse response =
          new BatchLoadRelationsResponse(
              results.build(),
              unprocessedIdentifiers != null ? unprocessedIdentifiers.build() : null);
      response.validate();
      return response;
    }
  }
}
