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
import java.util.Set;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;

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
  private final List<Action> columnProjections;
  private final Set<Integer> maskedFieldIds;

  private ReadRestrictions(Expression rowFilter, List<Action> columnProjections) {
    this.rowFilter = rowFilter;
    this.columnProjections = ImmutableList.copyOf(columnProjections);
    this.maskedFieldIds =
        this.columnProjections.isEmpty()
            ? ImmutableSet.of()
            : this.columnProjections.stream()
                .map(Action::fieldId)
                .collect(ImmutableSet.toImmutableSet());
  }

  public static ReadRestrictions empty() {
    return EMPTY;
  }

  public static ReadRestrictions of(Expression rowFilter, List<Action> columnProjections) {
    List<Action> actions = columnProjections == null ? ImmutableList.of() : columnProjections;
    if (rowFilter == null && actions.isEmpty()) {
      return EMPTY;
    }
    return new ReadRestrictions(rowFilter, actions);
  }

  public Expression rowFilter() {
    return rowFilter;
  }

  public List<Action> columnProjections() {
    return columnProjections;
  }

  public boolean isEmpty() {
    return rowFilter == null && columnProjections.isEmpty();
  }

  /** Field ids covered by a column projection action. */
  public Set<Integer> maskedFieldIds() {
    return maskedFieldIds;
  }
}
