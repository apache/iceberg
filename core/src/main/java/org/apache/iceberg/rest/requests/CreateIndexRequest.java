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
import java.util.Map;
import org.apache.iceberg.index.IndexType;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.RESTRequest;

/** A REST request to create a new index on a table. */
public class CreateIndexRequest implements RESTRequest {

  private String name;
  private IndexType type;
  private List<Integer> indexColumnIds;
  private List<Integer> optimizedColumnIds;
  private String location;
  private Map<String, String> properties;

  public CreateIndexRequest() {
    // Required for Jackson deserialization
  }

  private CreateIndexRequest(
      String name,
      IndexType type,
      List<Integer> indexColumnIds,
      List<Integer> optimizedColumnIds,
      String location,
      Map<String, String> properties) {
    this.name = name;
    this.type = type;
    this.indexColumnIds = indexColumnIds;
    this.optimizedColumnIds = optimizedColumnIds;
    this.location = location;
    this.properties = properties;
    validate();
  }

  @Override
  public void validate() {
    Preconditions.checkArgument(
        name != null && !name.isEmpty(), "Invalid index name: null or empty");
    Preconditions.checkArgument(type != null, "Invalid index type: null");
    Preconditions.checkArgument(
        indexColumnIds != null && !indexColumnIds.isEmpty(),
        "Invalid index column IDs: null or empty");
  }

  public String name() {
    return name;
  }

  public IndexType type() {
    return type;
  }

  public List<Integer> indexColumnIds() {
    return indexColumnIds != null ? indexColumnIds : ImmutableList.of();
  }

  public List<Integer> optimizedColumnIds() {
    return optimizedColumnIds != null ? optimizedColumnIds : ImmutableList.of();
  }

  public String location() {
    return location;
  }

  public Map<String, String> properties() {
    return properties != null ? properties : ImmutableMap.of();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("name", name)
        .add("type", type)
        .add("indexColumnIds", indexColumnIds)
        .add("optimizedColumnIds", optimizedColumnIds)
        .add("location", location)
        .add("properties", properties)
        .toString();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private String name;
    private IndexType type;
    private final List<Integer> indexColumnIds = Lists.newArrayList();
    private final List<Integer> optimizedColumnIds = Lists.newArrayList();
    private String location;
    private final Map<String, String> properties = Maps.newHashMap();

    private Builder() {}

    public Builder withName(String indexName) {
      this.name = indexName;
      return this;
    }

    public Builder withType(IndexType indexType) {
      this.type = indexType;
      return this;
    }

    public Builder withIndexColumnIds(List<Integer> columnIds) {
      this.indexColumnIds.clear();
      this.indexColumnIds.addAll(columnIds);
      return this;
    }

    public Builder addIndexColumnId(Integer columnId) {
      this.indexColumnIds.add(columnId);
      return this;
    }

    public Builder withOptimizedColumnIds(List<Integer> columnIds) {
      this.optimizedColumnIds.clear();
      this.optimizedColumnIds.addAll(columnIds);
      return this;
    }

    public Builder addOptimizedColumnId(Integer columnId) {
      this.optimizedColumnIds.add(columnId);
      return this;
    }

    public Builder withLocation(String indexLocation) {
      this.location = indexLocation;
      return this;
    }

    public Builder setProperty(String key, String value) {
      this.properties.put(key, value);
      return this;
    }

    public Builder setProperties(Map<String, String> props) {
      this.properties.putAll(props);
      return this;
    }

    public CreateIndexRequest build() {
      return new CreateIndexRequest(
          name,
          type,
          ImmutableList.copyOf(indexColumnIds),
          optimizedColumnIds.isEmpty() ? null : ImmutableList.copyOf(optimizedColumnIds),
          location,
          properties.isEmpty() ? null : ImmutableMap.copyOf(properties));
    }
  }
}
