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
import org.apache.iceberg.view.View;

/**
 * An optional catalog capability for loading tables and views with additional context.
 *
 * <p>When a table or view is loaded as part of resolving a view definition, the chain of
 * referencing views and other contextual information can be passed to the catalog. This enables
 * catalog servers to make authorization, credential-scoping, and auditing decisions.
 *
 * <p>Catalogs that do not need this context are not required to implement this interface.
 */
public interface SupportsContextualLoad {

  /**
   * Load a table with additional load context.
   *
   * @param identifier a table identifier
   * @param context the load context
   * @return an Iceberg table
   */
  Table loadTable(TableIdentifier identifier, LoadContext context);

  /**
   * Load a view with additional load context.
   *
   * @param identifier a view identifier
   * @param context the load context
   * @return an Iceberg view
   */
  View loadView(TableIdentifier identifier, LoadContext context);
}
