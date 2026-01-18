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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.UncheckedIOException;
import java.util.List;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionParser;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.RESTRequest;
import org.apache.iceberg.util.JsonUtil;

public class PlanTableScanRequest implements RESTRequest {
  private final Long snapshotId;
  private final List<String> select;
  private final JsonNode filterJson;
  private final boolean caseSensitive;
  private final boolean useSnapshotSchema;
  private final Long startSnapshotId;
  private final Long endSnapshotId;
  private final List<String> statsFields;
  private final Long minRowsRequested;

  public Long snapshotId() {
    return snapshotId;
  }

  public List<String> select() {
    return select;
  }

  /**
   * Returns the filter expression, deserializing it without schema context.
   *
   * <p>Note: This method does not perform type-aware deserialization and may not work correctly for
   * BINARY, FIXED, and DECIMAL types. Use {@link #filter(Schema)} instead for proper type handling.
   *
   * @return the filter expression, or null if no filter was specified
   * @deprecated since 1.11.0, will be removed in 1.12.0; use {@link #filter(Schema)} instead for
   *     proper type-aware deserialization
   */
  @Deprecated
  public Expression filter() {
    if (filterJson == null) {
      return null;
    }
    return ExpressionParser.fromJson(filterJson);
  }

  /**
   * Returns the filter expression, deserializing it with the provided schema for type inference.
   *
   * <p>This method should be preferred over {@link #filter()} as it properly handles BINARY, FIXED,
   * and DECIMAL types by using schema information for type-aware deserialization.
   *
   * @param schema the table schema to use for type-aware deserialization of filter values
   * @return the filter expression, or null if no filter was specified
   */
  public Expression filter(Schema schema) {
    if (filterJson == null) {
      return null;
    }
    return ExpressionParser.fromJson(filterJson, schema);
  }

  /**
   * Returns the raw filter JSON node, if available. Package-private for use by parser.
   *
   * @return the raw filter JSON, or null if no filter JSON was stored
   */
  JsonNode filterJson() {
    return filterJson;
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

  public Long minRowsRequested() {
    return minRowsRequested;
  }

  private PlanTableScanRequest(
      Long snapshotId,
      List<String> select,
      JsonNode filterJson,
      boolean caseSensitive,
      boolean useSnapshotSchema,
      Long startSnapshotId,
      Long endSnapshotId,
      List<String> statsFields,
      Long minRowsRequested) {
    this.snapshotId = snapshotId;
    this.select = select;
    this.filterJson = filterJson;
    this.caseSensitive = caseSensitive;
    this.useSnapshotSchema = useSnapshotSchema;
    this.startSnapshotId = startSnapshotId;
    this.endSnapshotId = endSnapshotId;
    this.statsFields = statsFields;
    this.minRowsRequested = minRowsRequested;
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

    if (null != minRowsRequested) {
      Preconditions.checkArgument(
          minRowsRequested >= 0L, "Invalid scan: minRowsRequested is negative");
    }

    if (null != filterJson) {
      Preconditions.checkArgument(
          filterJson.isBoolean() || filterJson.isObject(),
          "Cannot parse expression from non-object: %s",
          filterJson);
    }
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("snapshotId", snapshotId)
        .add("select", select)
        .add("filter", filterJson)
        .add("caseSensitive", caseSensitive)
        .add("useSnapshotSchema", useSnapshotSchema)
        .add("startSnapshotId", startSnapshotId)
        .add("endSnapshotId", endSnapshotId)
        .add("statsFields", statsFields)
        .add("minRowsRequested", minRowsRequested)
        .toString();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private Long snapshotId;
    private List<String> select;
    private JsonNode filterJson;
    private boolean caseSensitive = true;
    private boolean useSnapshotSchema = false;
    private Long startSnapshotId;
    private Long endSnapshotId;
    private List<String> statsFields;
    private Long minRowsRequested;

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

    /**
     * Sets the filter expression for the scan.
     *
     * @param expression the filter expression
     * @return this builder
     * @deprecated since 1.11.0, will be removed in 1.12.0; this method serializes the expression to
     *     JSON immediately, which may lose type information for BINARY, FIXED, and DECIMAL types
     */
    @Deprecated
    public Builder withFilter(Expression expression) {
      if (expression != null) {
        try {
          // Serialize expression to JSON immediately for deferred type-aware deserialization
          String jsonString = ExpressionParser.toJson(expression);
          this.filterJson = JsonUtil.mapper().readTree(jsonString);
        } catch (JsonProcessingException e) {
          throw new UncheckedIOException("Failed to serialize filter expression to JSON", e);
        }
      } else {
        this.filterJson = null;
      }
      return this;
    }

    /**
     * Sets the filter JSON node directly. Package-private for use by parser.
     *
     * @param filterJsonNode the filter as a JSON node
     * @return this builder
     */
    Builder withFilterJson(JsonNode filterJsonNode) {
      this.filterJson = filterJsonNode;
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

    public Builder withMinRowsRequested(Long rowsRequested) {
      this.minRowsRequested = rowsRequested;
      return this;
    }

    public PlanTableScanRequest build() {
      return new PlanTableScanRequest(
          snapshotId,
          select,
          filterJson,
          caseSensitive,
          useSnapshotSchema,
          startSnapshotId,
          endSnapshotId,
          statsFields,
          minRowsRequested);
    }
  }
}
