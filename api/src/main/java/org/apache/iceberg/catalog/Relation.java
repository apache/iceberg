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
 * A loaded catalog relation. A relation is either a table or a view. In the future, materialized
 * views may set both the table and view fields.
 */
public class Relation {
  private final CatalogObjectType objectType;
  private final Table table;
  private final View view;

  private Relation(CatalogObjectType objectType, Table table, View view) {
    this.objectType = objectType;
    this.table = table;
    this.view = view;
  }

  public static Relation forTable(Table table) {
    Preconditions.checkArgument(table != null, "Invalid table: null");
    return new Relation(CatalogObjectType.TABLE, table, null);
  }

  public static Relation forView(View view) {
    Preconditions.checkArgument(view != null, "Invalid view: null");
    return new Relation(CatalogObjectType.VIEW, null, view);
  }

  public CatalogObjectType objectType() {
    return objectType;
  }

  /** Returns the table, or null if this relation is a view. */
  public Table table() {
    return table;
  }

  /** Returns the view, or null if this relation is a table. */
  public View view() {
    return view;
  }
}
