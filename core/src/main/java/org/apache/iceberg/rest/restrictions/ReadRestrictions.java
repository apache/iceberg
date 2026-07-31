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
package org.apache.iceberg.rest.restrictions;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.functions.IcebergFunction;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

/**
 * Server-provided read restrictions for the authenticated principal.
 *
 * <p>Applies only to the principal identified by the request's authentication. An empty instance
 * (no row filter, no column projections) is equivalent to the property being absent from the
 * response.
 */
public class ReadRestrictions implements Serializable {

  private static final ReadRestrictions EMPTY = new ReadRestrictions(null, ImmutableList.of());

  private final Expression rowFilter;
  private final List<IcebergFunction<?, ?>> columnProjections;
  private final Set<Integer> maskedFieldIds;

  private ReadRestrictions(Expression rowFilter, List<IcebergFunction<?, ?>> columnProjections) {
    this.rowFilter = rowFilter;
    this.columnProjections = ImmutableList.copyOf(columnProjections);

    Set<Integer> fieldIds = Sets.newLinkedHashSet();
    List<Integer> duplicates = Lists.newArrayList();
    for (IcebergFunction<?, ?> projection : this.columnProjections) {
      if (!fieldIds.add(projection.fieldId())) {
        duplicates.add(projection.fieldId());
      }
    }

    // The spec requires a reader to fail when a field id carries more than one projection: the
    // actions do not compose and applying either one silently would be a policy decision.
    Preconditions.checkArgument(
        duplicates.isEmpty(),
        "Invalid read restrictions: duplicate column projections for field ids %s",
        duplicates);

    this.maskedFieldIds = ImmutableSet.copyOf(fieldIds);
  }

  public static ReadRestrictions empty() {
    return EMPTY;
  }

  public static ReadRestrictions of(
      Expression rowFilter, List<IcebergFunction<?, ?>> columnProjections) {
    List<IcebergFunction<?, ?>> actions =
        columnProjections == null ? ImmutableList.of() : columnProjections;
    if (rowFilter == null && actions.isEmpty()) {
      return EMPTY;
    }
    return new ReadRestrictions(rowFilter, actions);
  }

  public Expression rowFilter() {
    return rowFilter;
  }

  public List<IcebergFunction<?, ?>> columnProjections() {
    return columnProjections;
  }

  public boolean isEmpty() {
    return rowFilter == null && columnProjections.isEmpty();
  }

  /** Field ids covered by a column projection action. */
  public Set<Integer> maskedFieldIds() {
    return maskedFieldIds;
  }

  /**
   * Validates that every column projection targets a field id the table knows.
   *
   * <p>Checked against every schema the table has ever had rather than only the current one: field
   * ids are never reused, so membership in any schema identifies the column unambiguously, and a
   * time-travel read may legitimately be restricted on a column that has since been dropped.
   *
   * <p>Validating once here, where server-provided restrictions first meet a table, means every
   * reader inherits the check instead of re-deriving it per scan.
   *
   * @param schemas the table's schemas, keyed by schema id
   * @throws IllegalArgumentException if any projection targets a field id in none of the schemas
   */
  public void validate(Map<Integer, Schema> schemas) {
    List<Integer> unknownFieldIds =
        maskedFieldIds.stream()
            .filter(
                fieldId -> schemas.values().stream().noneMatch(s -> s.findField(fieldId) != null))
            .collect(Collectors.toList());

    Preconditions.checkArgument(
        unknownFieldIds.isEmpty(),
        "Invalid read restrictions: column projections reference unknown field ids %s",
        unknownFieldIds);
  }
}
