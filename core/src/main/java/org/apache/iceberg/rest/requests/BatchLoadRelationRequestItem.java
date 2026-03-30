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

import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * A single relation to load as part of a batch request. Optional {@link #etag()} and {@link
 * #snapshots()} parameters apply only when the resolved relation is a table.
 */
public class BatchLoadRelationRequestItem {

  private TableIdentifier identifier;
  private String etag;
  private String snapshots;

  public BatchLoadRelationRequestItem() {
    // Required for Jackson deserialization
  }

  private BatchLoadRelationRequestItem(TableIdentifier identifier, String etag, String snapshots) {
    this.identifier = identifier;
    this.etag = etag;
    this.snapshots = snapshots;
  }

  public void validate() {
    Preconditions.checkNotNull(identifier, "Invalid identifier: null");
  }

  public TableIdentifier identifier() {
    return identifier;
  }

  public String etag() {
    return etag;
  }

  public String snapshots() {
    return snapshots;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("identifier", identifier)
        .add("etag", etag)
        .add("snapshots", snapshots)
        .toString();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private TableIdentifier identifier;
    private String etag;
    private String snapshots;

    private Builder() {}

    public Builder withIdentifier(TableIdentifier ident) {
      this.identifier = ident;
      return this;
    }

    public Builder withEtag(String etagValue) {
      this.etag = etagValue;
      return this;
    }

    public Builder withSnapshots(String snapshotsValue) {
      this.snapshots = snapshotsValue;
      return this;
    }

    public BatchLoadRelationRequestItem build() {
      BatchLoadRelationRequestItem item =
          new BatchLoadRelationRequestItem(identifier, etag, snapshots);
      item.validate();
      return item;
    }
  }
}
