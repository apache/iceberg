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

import org.apache.hc.core5.http.HttpStatus;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Per-item result in a batch load response. The {@link #status()} field indicates the outcome: 200
 * for success with payload, 304 for not-modified (tables only), 404 for not found.
 */
public class BatchLoadRelationResultItem {

  private TableIdentifier identifier;
  private int status;
  private LoadRelationResponse result;
  private String etag;

  public BatchLoadRelationResultItem() {
    // Required for Jackson deserialization
  }

  private BatchLoadRelationResultItem(
      TableIdentifier identifier, int status, LoadRelationResponse result, String etag) {
    this.identifier = identifier;
    this.status = status;
    this.result = result;
    this.etag = etag;
  }

  public void validate() {
    Preconditions.checkNotNull(identifier, "Invalid identifier: null");
    Preconditions.checkArgument(
        status == HttpStatus.SC_OK
            || status == HttpStatus.SC_NOT_MODIFIED
            || status == HttpStatus.SC_NOT_FOUND,
        "Invalid status: %s",
        status);
    if (status == HttpStatus.SC_OK) {
      Preconditions.checkArgument(result != null, "Invalid result: null when status is %s", status);
    }
  }

  public TableIdentifier identifier() {
    return identifier;
  }

  public int status() {
    return status;
  }

  public LoadRelationResponse result() {
    return result;
  }

  public String etag() {
    return etag;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("identifier", identifier)
        .add("status", status)
        .add("result", result)
        .add("etag", etag)
        .toString();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private TableIdentifier identifier;
    private int status;
    private LoadRelationResponse result;
    private String etag;

    private Builder() {}

    public Builder withIdentifier(TableIdentifier ident) {
      this.identifier = ident;
      return this;
    }

    public Builder withStatus(int statusCode) {
      this.status = statusCode;
      return this;
    }

    public Builder withResult(LoadRelationResponse resultResponse) {
      this.result = resultResponse;
      return this;
    }

    public Builder withEtag(String etagValue) {
      this.etag = etagValue;
      return this;
    }

    public BatchLoadRelationResultItem build() {
      BatchLoadRelationResultItem item =
          new BatchLoadRelationResultItem(identifier, status, result, etag);
      item.validate();
      return item;
    }
  }
}
