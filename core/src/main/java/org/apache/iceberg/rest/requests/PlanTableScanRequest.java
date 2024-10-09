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
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.RESTRequest;

public class PlanTableScanRequest implements RESTRequest {
  private Long snapshotId;
  private List<String> select;
  private Expression filter;
  private Boolean caseSensitive;
  private Boolean useSnapshotSchema;
  private Long startSnapshotId;
  private Long endSnapshotId;
  private List<String> statsFields;

  public Long snapshotId() {
    return snapshotId;
  }

  public List<String> select() {
    return select;
  }

  public Expression filter() {
    return filter;
  }

  public Boolean caseSensitive() {
    return caseSensitive;
  }

  public Boolean useSnapshotSchema() {
    return useSnapshotSchema;
  }

  public Long startSnapshotId() {
    return startSnapshotId;
  }

  public Long endSnapshotId() {
    return endSnapshotId;
  }

  public List<String> statsFields() {
    return statsFields;
  }

  public PlanTableScanRequest() {
    // Needed for Jackson Deserialization.
  }

  public PlanTableScanRequest(
      Long snapshotId,
      List<String> select,
      Expression filter,
      Boolean caseSensitive,
      Boolean useSnapshotSchema,
      Long startSnapshotId,
      Long endSnapshotId,
      List<String> statsFields) {
    this.snapshotId = snapshotId;
    this.select = select;
    this.filter = filter;
    this.caseSensitive = caseSensitive;
    this.useSnapshotSchema = useSnapshotSchema;
    this.startSnapshotId = startSnapshotId;
    this.endSnapshotId = endSnapshotId;
    this.statsFields = statsFields;
  }

  @Override
  public void validate() {
    Preconditions.checkArgument(
        snapshotId() != null ^ (startSnapshotId() != null && endSnapshotId() != null),
        "Either snapshotId must be provided or both startSnapshotId and endSnapshotId must be provided");
  }

  public static class Builder {
    private Long snapshotId;
    private List<String> select;
    private Expression filter;
    private Boolean caseSensitive;
    private Boolean useSnapshotSchema;
    private Long startSnapshotId;
    private Long endSnapshotId;
    private List<String> statsFields;

    public Builder() {}

    public Builder withSnapshotId(Long withSnapshotId) {
      this.snapshotId = withSnapshotId;
      return this;
    }

    public Builder withSelect(List<String> withSelect) {
      this.select = withSelect;
      return this;
    }

    public Builder withFilter(Expression withFilter) {
      this.filter = withFilter;
      return this;
    }

    public Builder withCaseSensitive(Boolean withCaseSensitive) {
      this.caseSensitive = withCaseSensitive;
      return this;
    }

    public Builder withUseSnapshotSchema(Boolean withUseSnapshotSchema) {
      this.useSnapshotSchema = withUseSnapshotSchema;
      return this;
    }

    public Builder withStartSnapshotId(Long withStartSnapshotId) {
      this.startSnapshotId = withStartSnapshotId;
      return this;
    }

    public Builder withEndSnapshotId(Long withEndSnapshotId) {
      this.endSnapshotId = withEndSnapshotId;
      return this;
    }

    public Builder withStatsFields(List<String> withStatsFields) {
      this.statsFields = withStatsFields;
      return this;
    }

    public PlanTableScanRequest build() {
      return new PlanTableScanRequest(
          snapshotId,
          select,
          filter,
          caseSensitive,
          useSnapshotSchema,
          startSnapshotId,
          endSnapshotId,
          statsFields);
    }
  }
}
