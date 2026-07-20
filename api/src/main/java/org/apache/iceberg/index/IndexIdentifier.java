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

import java.util.Objects;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Identifies a secondary index by its source table and index name.
 *
 * <p>Format: {@code <namespace>.<table>.<indexName>}
 *
 * <p>Example: {@code db.orders.order_id_idx}
 */
public class IndexIdentifier {

  private final TableIdentifier tableIdentifier;
  private final String name;

  private IndexIdentifier(TableIdentifier tableIdentifier, String name) {
    this.tableIdentifier = tableIdentifier;
    this.name = name;
  }

  public static IndexIdentifier of(TableIdentifier tableIdentifier, String name) {
    Preconditions.checkNotNull(tableIdentifier, "tableIdentifier is required");
    Preconditions.checkArgument(
        name != null && !name.isEmpty(), "index name must be non-empty");
    return new IndexIdentifier(tableIdentifier, name);
  }

  public TableIdentifier tableIdentifier() {
    return tableIdentifier;
  }

  public String name() {
    return name;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof IndexIdentifier)) return false;
    IndexIdentifier that = (IndexIdentifier) o;
    return Objects.equals(tableIdentifier, that.tableIdentifier)
        && Objects.equals(name, that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tableIdentifier, name);
  }

  @Override
  public String toString() {
    return tableIdentifier + "." + name;
  }
}
