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
package org.apache.iceberg.connect.data;

import java.util.Objects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/** Immutable routing result representing a target Iceberg table for a record. */
public class RouteTarget {

  private final String tableName;
  private final boolean ignoreMissingTable;

  private RouteTarget(String tableName, boolean ignoreMissingTable) {
    Preconditions.checkNotNull(tableName, "Table name cannot be null");
    this.tableName = tableName;
    this.ignoreMissingTable = ignoreMissingTable;
  }

  public static RouteTarget of(String tableName) {
    return new RouteTarget(tableName, false);
  }

  public static RouteTarget of(String tableName, boolean ignoreMissingTable) {
    return new RouteTarget(tableName, ignoreMissingTable);
  }

  public String tableName() {
    return tableName;
  }

  public boolean ignoreMissingTable() {
    return ignoreMissingTable;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RouteTarget)) {
      return false;
    }
    RouteTarget that = (RouteTarget) o;
    return ignoreMissingTable == that.ignoreMissingTable
        && Objects.equals(tableName, that.tableName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tableName, ignoreMissingTable);
  }

  @Override
  public String toString() {
    return "RouteTarget{tableName='"
        + tableName
        + "', ignoreMissingTable="
        + ignoreMissingTable
        + "}";
  }
}
