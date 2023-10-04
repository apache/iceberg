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
import org.apache.iceberg.Schema;
import org.apache.iceberg.UpdateLocation;

/** Interface for view definition. */
public interface View {

  String name();

  /**
   * Return the {@link Schema schema} for this view.
   *
   * @return this table's schema
   */
  Schema schema();

  /**
   * Return a map of {@link Schema schema} for this view.
   *
   * @return this table's schema map
   */
  Map<Integer, Schema> schemas();

  /**
   * Get the current version for this view, or null if there are no versions.
   *
   * @return the current view version.
   */
  ViewVersion currentVersion();

  /**
   * Get the versions of this view.
   *
   * @return an Iterable of versions of this view.
   */
  Iterable<ViewVersion> versions();

  /**
   * Get a version in this view by ID.
   *
   * @param versionId version ID
   * @return a version, or null if the ID cannot be found
   */
  ViewVersion version(int versionId);

  /**
   * Get the version history of this table.
   *
   * @return a list of {@link ViewHistoryEntry}
   */
  List<ViewHistoryEntry> history();

  /**
   * Return a map of string properties for this view.
   *
   * @return this view's properties map
   */
  Map<String, String> properties();

  /**
   * Return the view's base location.
   *
   * @return this view's location
   */
  default String location() {
    throw new UnsupportedOperationException("Retrieving a view's location is not supported");
  }

  /**
   * Create a new {@link UpdateViewProperties} to update view properties.
   *
   * @return a new {@link UpdateViewProperties}
   */
  UpdateViewProperties updateProperties();

  /**
   * Create a new {@link ReplaceViewVersion} to replace the view's current version.
   *
   * @return a new {@link ReplaceViewVersion}
   */
  default ReplaceViewVersion replaceVersion() {
    throw new UnsupportedOperationException("Replacing a view's version is not supported");
  }

  /**
   * Create a new {@link UpdateLocation} to set the view's location.
   *
   * @return a new {@link UpdateLocation}
   */
  default UpdateLocation updateLocation() {
    throw new UnsupportedOperationException("Updating a view's location is not supported");
  }
}
