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

import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.view.View;

/**
 * A loaded catalog relation. A relation is either a table, a view, or a not-found marker. In the
 * future, materialized views may set both the table and view fields.
 */
public class Relation {
  private final TableIdentifier identifier;
  private final CatalogObjectType objectType;
  private final Table table;
  private final View view;

  private Relation(
      TableIdentifier identifier, CatalogObjectType objectType, Table table, View view) {
    this.identifier = identifier;
    this.objectType = objectType;
    this.table = table;
    this.view = view;
  }

  public static Relation forTable(TableIdentifier identifier, Table table) {
    Preconditions.checkArgument(identifier != null, "Invalid identifier: null");
    Preconditions.checkArgument(table != null, "Invalid table: null");
    return new Relation(identifier, CatalogObjectType.TABLE, table, null);
  }

  public static Relation forView(TableIdentifier identifier, View view) {
    Preconditions.checkArgument(identifier != null, "Invalid identifier: null");
    Preconditions.checkArgument(view != null, "Invalid view: null");
    return new Relation(identifier, CatalogObjectType.VIEW, null, view);
  }

  /** Create a relation representing a not-found object. */
  public static Relation notFound(TableIdentifier identifier) {
    Preconditions.checkArgument(identifier != null, "Invalid identifier: null");
    return new Relation(identifier, null, null, null);
  }

  public TableIdentifier identifier() {
    return identifier;
  }

  /** Returns the object type, or null if the object was not found. */
  public CatalogObjectType objectType() {
    return objectType;
  }

  /** Returns the table, or null if this relation is a view or not found. */
  public Table table() {
    return table;
  }

  /** Returns the view, or null if this relation is a table or not found. */
  public View view() {
    return view;
  }

  /** Returns true if the object was not found. */
  public boolean isNotFound() {
    return objectType == null;
  }
}
