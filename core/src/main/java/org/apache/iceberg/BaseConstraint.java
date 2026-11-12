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

import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

abstract class BaseConstraint implements Constraint {
  private final int id;
  private final String name;
  private final boolean enforced;
  private final long timestampMillis;

  private BaseConstraint(int id, String name, boolean enforced, long timestampMillis) {
    Preconditions.checkArgument(
        name != null && !name.isEmpty(), "Invalid constraint name: null or empty");
    this.id = id;
    this.name = name;
    this.enforced = enforced;
    this.timestampMillis = timestampMillis;
  }

  static CheckConstraint check(
      int id, String name, boolean enforced, long timestampMillis, String expressionJson) {
    return new Check(id, name, enforced, timestampMillis, expressionJson);
  }

  static UniqueConstraint unique(
      int id, String name, boolean enforced, long timestampMillis, List<Integer> fieldIds) {
    return new Unique(id, name, enforced, timestampMillis, fieldIds);
  }

  static PrimaryKeyConstraint primaryKey(
      int id, String name, boolean enforced, long timestampMillis, List<Integer> fieldIds) {
    return new PrimaryKey(id, name, enforced, timestampMillis, fieldIds);
  }

  @Override
  public int id() {
    return id;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public boolean enforced() {
    return enforced;
  }

  @Override
  public long timestampMillis() {
    return timestampMillis;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    } else if (other == null || getClass() != other.getClass()) {
      return false;
    }

    BaseConstraint that = (BaseConstraint) other;
    return id == that.id
        && name.equals(that.name)
        && enforced == that.enforced
        && timestampMillis == that.timestampMillis
        && Objects.equals(payload(), that.payload());
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, type(), name, enforced, timestampMillis, payload());
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("id", id)
        .add("type", type())
        .add("name", name)
        .add("enforced", enforced)
        .add("timestamp-ms", timestampMillis)
        .add("payload", payload())
        .toString();
  }

  /** Returns the type-specific definition, used for equality. */
  abstract Object payload();

  static class Check extends BaseConstraint implements CheckConstraint {
    // kept as JSON until ExpressionParser supports ID references
    private final String expressionJson;

    private Check(
        int id, String name, boolean enforced, long timestampMillis, String expressionJson) {
      super(id, name, enforced, timestampMillis);
      Preconditions.checkArgument(
          expressionJson != null, "Invalid check constraint %s: missing expression", name);
      this.expressionJson = expressionJson;
    }

    @Override
    public ConstraintType type() {
      return ConstraintType.CHECK;
    }

    String expressionJson() {
      return expressionJson;
    }

    @Override
    Object payload() {
      return expressionJson;
    }
  }

  abstract static class Key extends BaseConstraint implements KeyConstraint {
    private final List<Integer> fieldIds;

    private Key(
        int id, String name, boolean enforced, long timestampMillis, List<Integer> fieldIds) {
      super(id, name, enforced, timestampMillis);
      Preconditions.checkArgument(
          fieldIds != null && !fieldIds.isEmpty(),
          "Invalid %s constraint %s: field IDs must not be null or empty",
          type(),
          name);
      Set<Integer> seen = Sets.newHashSet();
      for (Integer fieldId : fieldIds) {
        Preconditions.checkArgument(
            fieldId != null, "Invalid %s constraint %s: null field ID", type(), name);
        Preconditions.checkArgument(
            seen.add(fieldId),
            "Invalid %s constraint %s: duplicate field ID %s",
            type(),
            name,
            fieldId);
      }

      this.fieldIds = ImmutableList.copyOf(fieldIds);
    }

    @Override
    public List<Integer> fieldIds() {
      return fieldIds;
    }

    @Override
    Object payload() {
      return fieldIds;
    }
  }

  static class Unique extends Key implements UniqueConstraint {
    private Unique(
        int id, String name, boolean enforced, long timestampMillis, List<Integer> fieldIds) {
      super(id, name, enforced, timestampMillis, fieldIds);
    }

    @Override
    public ConstraintType type() {
      return ConstraintType.UNIQUE;
    }
  }

  static class PrimaryKey extends Key implements PrimaryKeyConstraint {
    private PrimaryKey(
        int id, String name, boolean enforced, long timestampMillis, List<Integer> fieldIds) {
      super(id, name, enforced, timestampMillis, fieldIds);
    }

    @Override
    public ConstraintType type() {
      return ConstraintType.PRIMARY_KEY;
    }
  }
}
