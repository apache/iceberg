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
package org.apache.iceberg.index;

import java.util.List;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;

public class IndexRequirements {

  private IndexRequirements() {}

  public static List<IndexRequirement> forCreateIndex(List<IndexUpdate> metadataUpdates) {
    Preconditions.checkArgument(null != metadataUpdates, "Invalid metadata updates: null");
    Builder builder = new Builder();
    builder.require(new IndexRequirement.AssertIndexDoesNotExist());
    metadataUpdates.forEach(builder::update);
    return builder.build();
  }

  public static List<IndexRequirement> forReplaceIndex(
      IndexMetadata base, List<IndexUpdate> metadataUpdates) {
    Preconditions.checkArgument(null != base, "Invalid index metadata: null");
    Preconditions.checkArgument(null != metadataUpdates, "Invalid metadata updates: null");
    Builder builder = new Builder();
    builder.require(new IndexRequirement.AssertIndexUUID(base.uuid()));
    metadataUpdates.forEach(builder::update);
    return builder.build();
  }

  private static class Builder {
    private final ImmutableList.Builder<IndexRequirement> requirements = ImmutableList.builder();

    private Builder() {}

    private Builder require(IndexRequirement requirement) {
      Preconditions.checkArgument(requirement != null, "Invalid requirement: null");
      requirements.add(requirement);
      return this;
    }

    private Builder update(IndexUpdate update) {
      Preconditions.checkArgument(update != null, "Invalid update: null");

      // No check at this point
      return this;
    }

    private List<IndexRequirement> build() {
      return requirements.build();
    }
  }
}
