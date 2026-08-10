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
package org.apache.iceberg.io;

/**
 * Extension of {@link SupportsPrefixOperations} for FileIO implementations that can list a single
 * directory level natively (e.g. delimited listing on object stores).
 */
public interface SupportsShallowPrefixOperations extends SupportsPrefixOperations {

  /**
   * List the immediate children of a prefix — one directory level down.
   *
   * <p>For hierarchical file systems this returns direct file and sub-directory children of the
   * given directory. For key/value object stores this maps to a delimited listing where
   * sub-prefixes correspond to distinct path segments appearing immediately after the prefix.
   *
   * @param prefix prefix to list; hierarchical stores may require this to match a directory
   * @return files and sub-prefixes found immediately under the given prefix
   */
  PrefixListing listImmediate(String prefix);
}
