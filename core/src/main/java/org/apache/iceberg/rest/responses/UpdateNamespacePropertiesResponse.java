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

import java.util.Collection;
import java.util.List;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.rest.RESTResponse;

/** A REST response to a request to set and/or remove properties on a namespace. */
public class UpdateNamespacePropertiesResponse implements RESTResponse {

  // List of namespace property keys that were removed
  private List<String> removed;
  // List of namespace property keys that were added or updated
  private List<String> updated;
  // List of properties that were requested for removal that were not found in the namespace's
  // properties
  private List<String> missing;

  public UpdateNamespacePropertiesResponse() {
    // Required for Jackson deserialization
  }

  private UpdateNamespacePropertiesResponse(
      List<String> removed, List<String> updated, List<String> missing) {
    this.removed = removed;
    this.updated = updated;
    this.missing = missing;
    validate();
  }

  @Override
  public void validate() {}

  public List<String> removed() {
    return removed != null ? removed : ImmutableList.of();
  }

  public List<String> updated() {
    return updated != null ? updated : ImmutableList.of();
  }

  public List<String> missing() {
    return missing != null ? missing : ImmutableList.of();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("removed", removed)
        .add("updates", updated)
        .add("missing", missing)
        .toString();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private final ImmutableSet.Builder<String> removedBuilder = ImmutableSet.builder();
    private final ImmutableSet.Builder<String> updatedBuilder = ImmutableSet.builder();
    private final ImmutableSet.Builder<String> missingBuilder = ImmutableSet.builder();

    private Builder() {}

    public Builder addMissing(String key) {
      Preconditions.checkNotNull(key, "Invalid missing property: null");
      missingBuilder.add(key);
      return this;
    }

    public Builder addMissing(Collection<String> missing) {
      Preconditions.checkNotNull(missing, "Invalid missing property list: null");
      Preconditions.checkArgument(!missing.contains(null), "Invalid missing property: null");
      missingBuilder.addAll(missing);
      return this;
    }

    public Builder addRemoved(String key) {
      Preconditions.checkNotNull(key, "Invalid removed property: null");
      removedBuilder.add(key);
      return this;
    }

    public Builder addRemoved(Collection<String> removed) {
      Preconditions.checkNotNull(removed, "Invalid removed property list: null");
      Preconditions.checkArgument(!removed.contains(null), "Invalid removed property: null");
      removedBuilder.addAll(removed);
      return this;
    }

    public Builder addUpdated(String key) {
      Preconditions.checkNotNull(key, "Invalid updated property: null");
      updatedBuilder.add(key);
      return this;
    }

    public Builder addUpdated(Collection<String> updated) {
      Preconditions.checkNotNull(updated, "Invalid updated property list: null");
      Preconditions.checkArgument(!updated.contains(null), "Invalid updated property: null");
      updatedBuilder.addAll(updated);
      return this;
    }

    public UpdateNamespacePropertiesResponse build() {
      List<String> removed = removedBuilder.build().asList();
      List<String> updated = updatedBuilder.build().asList();
      List<String> missing = missingBuilder.build().asList();
      return new UpdateNamespacePropertiesResponse(removed, updated, missing);
    }
  }
}
