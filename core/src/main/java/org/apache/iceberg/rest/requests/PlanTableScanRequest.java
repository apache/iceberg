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
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.RESTRequest;

public class PlanTableScanRequest implements RESTRequest {
  private final Long snapshotId;
  private final List<String> select;
  private final Expression filter;
  private final boolean caseSensitive;
  private final boolean useSnapshotSchema;
  private final Long startSnapshotId;
  private final Long endSnapshotId;
  private final List<String> statsFields;

  public Long snapshotId() {
    return snapshotId;
  }

  public List<String> select() {
    return select;
  }

  public Expression filter() {
    return filter;
  }

  public boolean caseSensitive() {
    return caseSensitive;
  }

  public boolean useSnapshotSchema() {
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

  private PlanTableScanRequest(
      Long snapshotId,
      List<String> select,
      Expression filter,
      boolean caseSensitive,
      boolean useSnapshotSchema,
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
    validate();
  }

  @Override
  public void validate() {
    if (null != snapshotId) {
      Preconditions.checkArgument(
          null == startSnapshotId && null == endSnapshotId,
          "Invalid scan: cannot provide both snapshotId and startSnapshotId/endSnapshotId");
    }

    if (null != startSnapshotId || null != endSnapshotId) {
      Preconditions.checkArgument(
          null != startSnapshotId && null != endSnapshotId,
          "Invalid incremental scan: startSnapshotId and endSnapshotId is required");
    }
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("snapshotId", snapshotId)
        .add("select", select)
        .add("filter", filter)
        .add("caseSensitive", caseSensitive)
        .add("useSnapshotSchema", useSnapshotSchema)
        .add("startSnapshotId", startSnapshotId)
        .add("endSnapshotId", endSnapshotId)
        .add("statsFields", statsFields)
        .toString();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private Long snapshotId;
    private List<String> select;
    private Expression filter;
    private boolean caseSensitive = true;
    private boolean useSnapshotSchema = false;
    private Long startSnapshotId;
    private Long endSnapshotId;
    private List<String> statsFields;

    /**
     * @deprecated since 1.11.0, visibility will be reduced in 1.12.0; use {@link
     *     PlanTableScanRequest#builder()} instead.
     */
    @Deprecated
    public Builder() {}

    public Builder withSnapshotId(Long withSnapshotId) {
      this.snapshotId = withSnapshotId;
      return this;
    }

    public Builder withSelect(List<String> projection) {
      this.select = projection;
      return this;
    }

    public Builder withFilter(Expression expression) {
      this.filter = expression;
      return this;
    }

    public Builder withCaseSensitive(boolean value) {
      this.caseSensitive = value;
      return this;
    }

    public Builder withUseSnapshotSchema(boolean snapshotSchema) {
      this.useSnapshotSchema = snapshotSchema;
      return this;
    }

    public Builder withStartSnapshotId(Long startingSnapshotId) {
      this.startSnapshotId = startingSnapshotId;
      return this;
    }

    public Builder withEndSnapshotId(Long endingSnapshotId) {
      this.endSnapshotId = endingSnapshotId;
      return this;
    }

    public Builder withStatsFields(List<String> fields) {
      this.statsFields = fields;
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
