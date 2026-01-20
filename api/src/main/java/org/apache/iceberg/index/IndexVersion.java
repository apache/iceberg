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

import java.util.Map;

/**
 * A version of the index at a point in time.
 *
 * <p>A version consists of index metadata and user-supplied properties.
 *
 * <p>Versions are created by index operations, like Create and Alter.
 */
public interface IndexVersion {

  /**
   * Return this version's id.
   *
   * <p>Version ids are monotonically increasing.
   *
   * @return the version ID
   */
  int versionId();

  /**
   * Return this version's timestamp.
   *
   * <p>This timestamp is the same as those produced by {@link System#currentTimeMillis()}.
   *
   * @return a long timestamp in milliseconds
   */
  long timestampMillis();

  /**
   * Return the user-supplied properties for this version.
   *
   * <p>A map of index properties, represented as string-to-string pairs, supplied by the user.
   *
   * @return a map of string properties, or null if not set
   */
  Map<String, String> properties();
}
