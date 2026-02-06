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
import org.apache.iceberg.rest.RESTRequest;

/** A REST request to rename a table or a view. */
public class RenameTableRequest implements RESTRequest {

  private TableIdentifier source;
  private TableIdentifier destination;

  @SuppressWarnings("unused")
  public RenameTableRequest() {
    // Needed for Jackson Deserialization.
  }

  private RenameTableRequest(TableIdentifier source, TableIdentifier destination) {
    this.source = source;
    this.destination = destination;
    validate();
  }

  @Override
  public void validate() {
    Preconditions.checkArgument(source != null, "Invalid source table: null");
    Preconditions.checkArgument(destination != null, "Invalid destination table: null");
  }

  public TableIdentifier source() {
    return source;
  }

  public TableIdentifier destination() {
    return destination;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("source", source)
        .add("destination", destination)
        .toString();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private TableIdentifier source;
    private TableIdentifier destination;

    private Builder() {}

    public Builder withSource(TableIdentifier sourceTable) {
      Preconditions.checkNotNull(sourceTable, "Invalid source table identifier: null");
      this.source = sourceTable;
      return this;
    }

    public Builder withDestination(TableIdentifier destinationTable) {
      Preconditions.checkNotNull(destinationTable, "Invalid destination table identifier: null");
      this.destination = destinationTable;
      return this;
    }

    public RenameTableRequest build() {
      return new RenameTableRequest(source, destination);
    }
  }
}
