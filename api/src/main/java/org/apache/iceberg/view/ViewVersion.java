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

package org.apache.iceberg.view;

import java.util.List;
import java.util.Map;

/**
 * A version of the view at a point in time.
 * <p>
 * A version consists of a view metadata file.
 * <p>
 * Versions are created by view operations, like Create and Replace.
 */
public interface ViewVersion {
  /**
   * Return this version's ID.
   *
   * @return a long ID
   */
  int versionId();

  /**
   * Return this version's parent ID or null.
   *
   * @return a long ID for this version's parent, or null if it has no parent
   */
  Integer parentId();

  /**
   * Return this version's timestamp.
   * <p>
   * This timestamp is the same as those produced by {@link System#currentTimeMillis()}.
   *
   * @return a long timestamp in milliseconds
   */
  long timestampMillis();

  /**
   * Returns the version summary such as the name and genie-id of the operation that created that version of the view
   *
   * @return a version summary
   */
  Map<String, String> summary();

  /**
   * Returns the list of view representations
   * <p>
   * Must contain at least one representation.
   *
   * @return the list of view representations
   */
  List<ViewRepresentation> representations();
}
