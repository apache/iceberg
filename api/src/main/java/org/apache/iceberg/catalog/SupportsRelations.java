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
import java.util.Set;

/**
 * Catalog methods for loading relations (tables and views) using a single endpoint.
 *
 * <p>Loading a relation resolves the identifier server-side and returns a discriminated result that
 * indicates whether the object is a table or a view.
 */
public interface SupportsRelations {

  /**
   * Load a single relation by identifier.
   *
   * @param identifier a table or view identifier
   * @return the loaded relation
   * @throws org.apache.iceberg.exceptions.NotFoundException if no table or view exists for the
   *     identifier
   */
  Relation loadRelation(TableIdentifier identifier);

  /**
   * Load multiple relations in a single batch request. Relations that are not found (404) are
   * skipped and will not appear in the returned list.
   *
   * @param identifiers the set of identifiers to load
   * @return the list of loaded relations, excluding not-found identifiers
   */
  List<Relation> loadRelations(Set<TableIdentifier> identifiers);
}
