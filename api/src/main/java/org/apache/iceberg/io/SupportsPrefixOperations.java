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
 * This interface is intended as an extension for FileIO implementations to provide additional
 * prefix based operations that may be useful in performing supporting operations.
 */
public interface SupportsPrefixOperations extends FileIO {

  /**
   * Return an iterable of all files under a prefix.
   *
   * <p>Hierarchical file systems (e.g. HDFS) may impose additional restrictions like the prefix
   * must fully match a directory whereas key/value object stores may allow for arbitrary prefixes.
   *
   * @param prefix prefix to list
   * @return iterable of file information
   */
  Iterable<FileInfo> listPrefix(String prefix);

  /**
   * Delete all files under a prefix.
   *
   * <p>Hierarchical file systems (e.g. HDFS) may impose additional restrictions like the prefix
   * must fully match a directory whereas key/value object stores may allow for arbitrary prefixes.
   *
   * @param prefix prefix to delete
   */
  void deletePrefix(String prefix);
}
