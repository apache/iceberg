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

import java.util.Locale;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/** The type of a table {@link Constraint}. */
public enum ConstraintType {
  /** A predicate that every row must satisfy. */
  CHECK("check"),
  /** A set of fields whose values must be distinct across rows when no value is null. */
  UNIQUE("unique"),
  /** A set of required fields whose values must be distinct across rows. */
  PRIMARY_KEY("primary-key");

  private final String typeName;

  ConstraintType(String typeName) {
    this.typeName = typeName;
  }

  public String typeName() {
    return typeName;
  }

  public static ConstraintType fromTypeName(String typeName) {
    Preconditions.checkArgument(typeName != null, "Invalid constraint type: null");
    String lower = typeName.toLowerCase(Locale.ROOT);
    for (ConstraintType type : values()) {
      if (type.typeName.equals(lower)) {
        return type;
      }
    }

    throw new IllegalArgumentException(String.format("Invalid constraint type: %s", typeName));
  }

  @Override
  public String toString() {
    return typeName;
  }
}
