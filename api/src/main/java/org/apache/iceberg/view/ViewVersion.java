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
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * A version of the view at a point in time.
 *
 * <p>A version consists of a view metadata file.
 *
 * <p>Versions are created by view operations, like Create and Replace.
 */
public interface ViewVersion {

  /** Return this version's id. Version ids are monotonically increasing */
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
   * Return the version summary
   *
   * @return a version summary
   */
  Map<String, String> summary();

  /**
   * Return the list of other view representations.
   *
   * <p>May contain SQL view representations for other dialects.
   *
   * @return the list of view representations
   */
  List<ViewRepresentation> representations();

  /**
   * Return the operation which produced the view version
   *
   * @return the string operation which produced the view version
   */
  default String operation() {
    return summary().get("operation");
  }

  /** The query output schema at version create time, without aliases */
  int schemaId();

  /** The default catalog when the view is created. */
  default String defaultCatalog() {
    return null;
  }

  /** The default namespace to use when the SQL does not contain a namespace. */
  Namespace defaultNamespace();

  default void check() {
    Preconditions.checkArgument(
        summary().containsKey("operation"), "Invalid view version summary, missing operation");
  }
}
