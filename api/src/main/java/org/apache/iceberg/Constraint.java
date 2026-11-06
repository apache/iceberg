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
package org.apache.iceberg;

import java.io.Serializable;

/**
 * A constraint declares a property that a table's rows are expected to satisfy.
 *
 * <p>A constraint's definition is stored in table metadata and references the fields it applies to
 * by field ID, so it keeps applying after columns are renamed or reordered. The concrete subtype is
 * determined by {@link #type()}: a {@link CheckConstraint} carries a predicate, and a {@link
 * KeyConstraint} ({@link UniqueConstraint} or {@link PrimaryKeyConstraint}) carries field IDs.
 * Iceberg does not evaluate constraints; enforcement and validation are done by engines.
 *
 * <p>Constraints are supported in format v4 and later.
 */
public interface Constraint extends Serializable {
  /** Returns the ID of the constraint, assigned by Iceberg and unique within the table. */
  int id();

  /**
   * Returns the name of the constraint, unique within the table.
   *
   * <p>Names are for humans and must not be used to identify a constraint in metadata.
   */
  String name();

  /** Returns the constraint {@link ConstraintType type}. */
  ConstraintType type();

  /**
   * Returns whether writers must verify that the rows they add or modify satisfy this constraint.
   *
   * <p>When enforced, a writer that cannot verify the constraint must reject writes rather than add
   * unverified rows. Iceberg core does not enforce constraints.
   */
  boolean enforced();

  /**
   * Returns the time the constraint was created or last modified, in milliseconds since the Unix
   * epoch.
   *
   * <p>This value is informational and must not be used to decide whether a constraint applies to a
   * snapshot or whether it holds.
   */
  long timestampMillis();
}
