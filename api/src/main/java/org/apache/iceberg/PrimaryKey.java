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
import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

public class PrimaryKey implements Serializable {

  private static final PrimaryKey NON_PRIMARY_KEY = new PrimaryKey(null, 0, false, ImmutableList.of());

  private final Schema schema;
  private final int keyId;
  private final boolean enforceUniqueness;
  private final Integer[] sourceIds;

  private transient volatile List<Integer> sourceIdList;

  private PrimaryKey(Schema schema, int keyId, boolean enforceUniqueness, List<Integer> sourceIds) {
    this.schema = schema;
    this.keyId = keyId;
    this.enforceUniqueness = enforceUniqueness;
    this.sourceIds = sourceIds.toArray(new Integer[0]);
  }

  public Schema schema() {
    return schema;
  }

  public int keyId() {
    return keyId;
  }

  public boolean enforceUniqueness() {
    return enforceUniqueness;
  }

  public List<Integer> sourceIds() {
    if (sourceIdList == null) {
      synchronized (this) {
        if (sourceIdList == null) {
          this.sourceIdList = ImmutableList.copyOf(sourceIds);
        }
      }
    }
    return sourceIdList;
  }

  public boolean isNonPrimaryKey() {
    return sourceIds.length == 0;
  }

  public static PrimaryKey nonPrimaryKey() {
    return NON_PRIMARY_KEY;
  }

  public boolean samePrimaryKey(PrimaryKey other) {
    return Arrays.equals(sourceIds, other.sourceIds);
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    } else if (!(other instanceof PrimaryKey)) {
      return false;
    }

    PrimaryKey that = (PrimaryKey) other;
    if (this.keyId != that.keyId) {
      return false;
    }

    if (this.enforceUniqueness != that.enforceUniqueness) {
      return false;
    }

    return Arrays.equals(sourceIds, that.sourceIds);
  }

  @Override
  public int hashCode() {
    int hash = 31 * keyId;
    hash = hash + (enforceUniqueness ? 1 : 0);
    hash += Arrays.hashCode(sourceIds);
    return hash;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("keyId", keyId)
        .add("enforceUniqueness", enforceUniqueness)
        .add("sourceIds", sourceIds())
        .toString();
  }

  public static Builder builderFor(Schema schema) {
    return new Builder(schema);
  }

  public static class Builder {
    private final Schema schema;
    private final List<Integer> sourceIds = Lists.newArrayList();
    // Default ID to 1 as 0 is reserved for non primary key.
    private int keyId = 1;
    private boolean enforceUniqueness = false;

    private Builder(Schema schema) {
      this.schema = schema;
    }

    public Builder withKeyId(int newKeyId) {
      this.keyId = newKeyId;
      return this;
    }

    public Builder withEnforceUniqueness(boolean enable) {
      this.enforceUniqueness = enable;
      return this;
    }

    public Builder addField(String name) {
      Types.NestedField column = schema.findField(name);

      Preconditions.checkNotNull(column, "Cannot find source column: %s", name);
      Preconditions.checkArgument(column.isRequired(), "Cannot add optional source field to primary key: %s", name);

      Type sourceType = column.type();
      ValidationException.check(sourceType.isPrimitiveType(), "Cannot add non-primitive field: %s", sourceType);

      sourceIds.add(column.fieldId());
      return this;
    }

    public Builder addField(int sourceId) {
      Types.NestedField column = schema.findField(sourceId);
      Preconditions.checkNotNull(column, "Cannot find source column: %s", sourceId);
      Preconditions.checkArgument(column.isRequired(), "Cannot add optional source field to primary key: %s", sourceId);

      Type sourceType = column.type();
      ValidationException.check(sourceType.isPrimitiveType(), "Cannot add non-primitive field: %s", sourceType);

      sourceIds.add(sourceId);
      return this;
    }

    public PrimaryKey build() {
      if (keyId == 0 && sourceIds.size() != 0) {
        throw new IllegalArgumentException("Primary key ID 0 is reserved for non-primary key");
      }
      if (sourceIds.size() == 0 && keyId != 0) {
        throw new IllegalArgumentException("Non-primary key ID must be 0");
      }

      return new PrimaryKey(schema, keyId, enforceUniqueness, sourceIds);
    }
  }
}
