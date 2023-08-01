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
package org.apache.iceberg.catalog;

import java.util.List;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.UpdateRequirements;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.immutables.value.Value;

/**
 * This represents a commit to be applied for a single table with {@link UpdateRequirement}s to be
 * validated and {@link MetadataUpdate}s that have been applied. The {@link UpdateRequirement}s and
 * {@link MetadataUpdate}s can be derived from table's base and updated {@link TableMetadata} when
 * using {@link TableCommit#create(TableIdentifier, TableMetadata, TableMetadata)}.
 */
@Value.Immutable
public interface TableCommit {
  TableIdentifier identifier();

  List<UpdateRequirement> requirements();

  List<MetadataUpdate> updates();

  /**
   * This creates a {@link TableCommit} instance to be applied for a single table with {@link
   * UpdateRequirement}s to be validated and {@link MetadataUpdate}s that have been applied.
   *
   * @param identifier The {@link TableIdentifier} to create the {@link TableCommit} for.
   * @param base The base {@link TableMetadata} where {@link UpdateRequirement}s are derived from
   *     and used for validation.
   * @param updated The updated {@link TableMetadata} where {@link MetadataUpdate}s that have been
   *     applied are derived from.
   * @return A {@link TableCommit} instance to be applied for a single table
   */
  static TableCommit create(TableIdentifier identifier, TableMetadata base, TableMetadata updated) {
    Preconditions.checkArgument(null != identifier, "Invalid table identifier: null");
    Preconditions.checkArgument(null != base && null != updated, "Invalid table metadata: null");
    Preconditions.checkArgument(
        base.uuid().equals(updated.uuid()),
        "UUID of base (%s) and updated (%s) table metadata does not match",
        base.uuid(),
        updated.uuid());

    return ImmutableTableCommit.builder()
        .identifier(identifier)
        .requirements(UpdateRequirements.forUpdateTable(base, updated.changes()))
        .updates(updated.changes())
        .build();
  }
}
