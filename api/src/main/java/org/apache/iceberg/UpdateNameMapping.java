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

import org.apache.iceberg.mapping.NameMapping;

/**
 * API for updating name mapping.
 */
public interface UpdateNameMapping extends PendingUpdate<NameMapping> {
  /**
   * Add a set of aliases to an existing column.
   *
   * @param name    name of the column for which aliases will be added to
   * @param aliases set of aliases that need to be added to the column
   * @return this for method chaining
   */
  UpdateNameMapping addAliases(String name, Iterable<String> aliases);

  /**
   * Add a set of aliases to a nested struct.
   * <p>
   * The parent name is used to find the parent using {@link Schema#findField(String)}. If parent
   * identifies itself as a struct and contains the column name as a nested field, then aliases
   * are added for this field. If it identifies as a list, the aliases are added to the list element struct, and if
   * it identifies as a map, the aliases are added to the map's value struct.
   * </p>
   *
   * @param parent  name of the parent struct
   * @param name    name of the column for which the aliases will be added to
   * @param aliases set of aliases for the new column
   * @return this for method chaining
   * @throws IllegalArgumentException If parent doesn't identify a struct
   */
  UpdateNameMapping addAliases(
      String parent, String name, Iterable<String> aliases) throws IllegalArgumentException;
}
