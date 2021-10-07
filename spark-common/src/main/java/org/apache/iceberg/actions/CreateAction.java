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

package org.apache.iceberg.actions;

import java.util.Map;

/**
 * @deprecated since 0.12.0, will be removed in 0.13.0; use {@link SnapshotTable} or {@link MigrateTable} instead.
 */
@Deprecated
public interface CreateAction extends Action<CreateAction, Long> {

  /**
   * Adds additional properties to the newly created Iceberg table. Any properties with
   * the same key name will be overwritten.
   * @param properties a map of properties to be included
   * @return this for chaining
   */
  CreateAction withProperties(Map<String, String> properties);

  /**
   * Adds an additional property to the newly created Iceberg table. Any properties
   * with the same key name will be overwritten.
   * @param key the key of the property to add
   * @param value the value of the property to add
   * @return this for chaining
   */
  CreateAction withProperty(String key, String value);
}
