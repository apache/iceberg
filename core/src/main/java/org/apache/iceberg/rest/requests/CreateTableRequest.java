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
import org.apache.iceberg.UnboundPartitionSpec;
import org.apache.iceberg.UnboundSortOrder;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.RESTRequest;

/**
 * A REST request to create a namespace, with an optional set of properties.
 */
public class CreateTableRequest implements RESTRequest {

  private String name;
  private String location;
  private Schema schema;
  private UnboundPartitionSpec spec;
  private UnboundSortOrder order;
  private Map<String, String> properties;
  private Boolean stageCreate = false;

  public CreateTableRequest() {
    // Needed for Jackson Deserialization.
  }

  private CreateTableRequest(String name, String location, Schema schema, PartitionSpec spec, SortOrder order,
                             Map<String, String> properties, boolean stageCreate) {
    this.name = name;
    this.location = location;
    this.schema = schema;
    this.spec = spec != null ? spec.toUnbound() : null;
    this.order = order != null ? order.toUnbound() : null;
    this.properties = properties;
    this.stageCreate = stageCreate;
    validate();
  }

  @Override
  public void validate() {
    Preconditions.checkArgument(name != null, "Invalid table name: null");
    Preconditions.checkArgument(schema != null, "Invalid schema: null");
    Preconditions.checkArgument(stageCreate != null, "Invalid stageCreate flag: null");
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
    return spec != null ? spec.bind(schema) : null;
  }

  public SortOrder writeOrder() {
    return order != null ? order.bind(schema) : null;
  }

  public Map<String, String> properties() {
    return properties != null ? properties : ImmutableMap.of();
  }

  public boolean stageCreate() {
    return stageCreate;
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
    private boolean stageCreate = false;

    private Builder() {
    }

    public Builder withName(String tableName) {
      Preconditions.checkNotNull(tableName, "Invalid name: null");
      this.name = tableName;
      return this;
    }

    public Builder withLocation(String newLocation) {
      this.location = newLocation;
      return this;
    }

    public Builder setProperty(String property, String value) {
      Preconditions.checkArgument(property != null, "Invalid property: null");
      Preconditions.checkArgument(value != null, "Invalid value for property %s: null", property);
      properties.put(property, value);
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

    public Builder stageCreate() {
      this.stageCreate = true;
      return this;
    }

    public CreateTableRequest build() {
      return new CreateTableRequest(name, location, schema, spec, order, properties.build(), stageCreate);
    }
  }
}
