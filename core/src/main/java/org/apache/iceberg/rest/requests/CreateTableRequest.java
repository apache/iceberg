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

import java.util.Map;
import java.util.Objects;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/**
 * A REST request to create a namespace, with an optional set of properties.
 */
public class CreateTableRequest {

  private String name;
  private String location;
  private Schema schema;
  private PartitionSpec spec;
  private SortOrder order;
  private Map<String, String> properties;

  public CreateTableRequest() {
    // Needed for Jackson Deserialization.
  }

  private CreateTableRequest(String name, String location, Schema schema, PartitionSpec spec, SortOrder order,
                             Map<String, String> properties) {
    this.name = name;
    this.location = location;
    this.schema = schema;
    this.spec = spec;
    this.order = order;
    this.properties = properties;
    validate();
  }

  public CreateTableRequest validate() {
    Preconditions.checkArgument(name != null, "Invalid table name: null");
    Preconditions.checkArgument(schema != null, "Invalid schema: null");
    return this;
  }

  public String name() {
    return name;
  }

  public String location() {
    return location;
  }

  public Schema schema() {
    return schema;
  }

  public PartitionSpec spec() {
    return spec;
  }

  public SortOrder writeOrder() {
    return order;
  }

  public Map<String, String> properties() {
    return properties != null ? properties : ImmutableMap.of();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("name", name)
        .add("location", location)
        .add("properties", properties)
        .add("schema", schema)
        .add("spec", spec)
        .add("order", order)
        .toString();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private String name;
    private String location;
    private Schema schema;
    private PartitionSpec spec;
    private SortOrder order;
    private final ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();

    private Builder() {
    }

    public Builder withName(String tableName) {
      Preconditions.checkNotNull(tableName, "Invalid name: null");
      this.name = tableName;
      return this;
    }

    public Builder withLocation(String location) {
      this.location = location;
      return this;
    }

    public Builder setProperty(String name, String value) {
      Preconditions.checkArgument(name != null, "Invalid property: null");
      Preconditions.checkArgument(value != null, "Invalid value for property %s: null", name);
      properties.put(name, value);
      return this;
    }

    public Builder setProperties(Map<String, String> props) {
      Preconditions.checkNotNull(props, "Invalid collection of properties: null");
      Preconditions.checkArgument(!props.containsKey(null), "Invalid property: null");
      Preconditions.checkArgument(!props.containsValue(null),
          "Invalid value for properties %s: null", Maps.filterValues(props, Objects::isNull).keySet());
      properties.putAll(props);
      return this;
    }

    public Builder withSchema(Schema tableSchema) {
      Preconditions.checkNotNull(tableSchema, "Invalid schema: null");
      this.schema = tableSchema;
      return this;
    }

    public Builder withPartitionSpec(PartitionSpec tableSpec) {
      this.spec = tableSpec;
      return this;
    }

    public Builder withWriteOrder(SortOrder writeOrder) {
      this.order = writeOrder;
      return this;
    }

    public CreateTableRequest build() {
      return new CreateTableRequest(name, location, schema, spec, order, properties.build());
    }
  }
}
