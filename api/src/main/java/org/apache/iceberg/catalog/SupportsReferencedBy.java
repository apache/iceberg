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

import java.util.List;
import org.apache.iceberg.Table;
import org.apache.iceberg.view.View;

/** Catalog methods for loading tables and views with a referenced-by view chain. */
public interface SupportsReferencedBy {

  /**
   * Load a table with the referenced-by view chain.
   *
   * @param identifier a table identifier
   * @param referencedBy ordered list of view identifiers from outermost to innermost
   * @return an iceberg table
   */
  Table loadTable(TableIdentifier identifier, List<TableIdentifier> referencedBy);

  /**
   * Load a view with the referenced-by view chain.
   *
   * @param identifier a view identifier
   * @param referencedBy ordered list of view identifiers from outermost to innermost
   * @return an iceberg view
   */
  View loadView(TableIdentifier identifier, List<TableIdentifier> referencedBy);
}
