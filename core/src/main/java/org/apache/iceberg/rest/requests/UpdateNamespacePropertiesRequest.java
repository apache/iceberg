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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.iceberg.exceptions.UnprocessableEntityException;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.rest.RESTRequest;

/** A REST request to set and/or remove properties on a namespace. */
public class UpdateNamespacePropertiesRequest implements RESTRequest {

  private List<String> removals;
  private Map<String, String> updates;

  public UpdateNamespacePropertiesRequest() {
    // Required for Jackson deserialization.
  }

  private UpdateNamespacePropertiesRequest(List<String> removals, Map<String, String> updates) {
    this.removals = removals;
    this.updates = updates;
    validate();
  }

  @Override
  public void validate() {
    Set<String> commonKeys = Sets.intersection(updates().keySet(), Sets.newHashSet(removals()));
    if (!commonKeys.isEmpty()) {
      throw new UnprocessableEntityException(
          "Invalid namespace update, cannot simultaneously set and remove keys: %s", commonKeys);
    }
  }

  public List<String> removals() {
    return removals == null ? ImmutableList.of() : removals;
  }

  public Map<String, String> updates() {
    return updates == null ? ImmutableMap.of() : updates;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("removals", removals)
        .add("updates", updates)
        .toString();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private final ImmutableSet.Builder<String> removalsBuilder = ImmutableSet.builder();
    private final ImmutableMap.Builder<String, String> updatesBuilder = ImmutableMap.builder();

    private Builder() {}

    public Builder remove(String removal) {
      Preconditions.checkNotNull(removal, "Invalid property to remove: null");
      removalsBuilder.add(removal);
      return this;
    }

    public Builder removeAll(Collection<String> removals) {
      Preconditions.checkNotNull(removals, "Invalid list of properties to remove: null");
      Preconditions.checkArgument(!removals.contains(null), "Invalid property to remove: null");
      removalsBuilder.addAll(removals);
      return this;
    }

    public Builder update(String key, String value) {
      Preconditions.checkNotNull(key, "Invalid property to update: null");
      Preconditions.checkNotNull(
          value, "Invalid value to update for key [%s]: null. Use remove instead", key);
      updatesBuilder.put(key, value);
      return this;
    }

    public Builder updateAll(Map<String, String> updates) {
      Preconditions.checkNotNull(updates, "Invalid collection of properties to update: null");
      Preconditions.checkArgument(!updates.containsKey(null), "Invalid property to update: null");
      Preconditions.checkArgument(
          !updates.containsValue(null),
          "Invalid value to update for properties %s: null. Use remove instead",
          Maps.filterValues(updates, Objects::isNull).keySet());
      updatesBuilder.putAll(updates);
      return this;
    }

    public UpdateNamespacePropertiesRequest build() {
      List<String> removals = removalsBuilder.build().asList();
      Map<String, String> updates = updatesBuilder.build();

      return new UpdateNamespacePropertiesRequest(removals, updates);
    }
  }
}
