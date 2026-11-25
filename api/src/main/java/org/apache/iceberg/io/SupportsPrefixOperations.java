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
   * Lists the files and subdirectories directly below a directory prefix.
   *
   * <p>The separator is always {@code /}, and {@code prefix} must end in {@code /}. Files below
   * subdirectories are not returned. Directory marker objects equal to {@code prefix} are omitted.
   * Subdirectory prefixes are unique across pages, end in {@code /}, and can be passed to this
   * method to list their children.
   *
   * <p>Callers must check {@link #supportsDirectoryListing(String)} before calling this method.
   *
   * @param prefix directory prefix ending in {@code /}
   * @return iterable of pages containing immediate files and subdirectory prefixes
   * @throws IllegalArgumentException if the prefix is invalid for this implementation
   * @throws UnsupportedOperationException if directory listing is not supported
   */
  default Iterable<PrefixListingPage> listDirectory(String prefix) {
    throw new UnsupportedOperationException("Directory listing is not supported");
  }

  /**
   * Returns whether directory listing is supported for the given prefix.
   *
   * <p>Support can vary by prefix when a FileIO selects a storage implementation from the location.
   * Implementations may also restrict supported location forms. Callers must check each prefix they
   * intend to list.
   *
   * @param prefix directory prefix ending in {@code /}
   * @return {@code true} if directory listing is supported for this prefix, or {@code false} if the
   *     prefix is null or does not end in {@code /}
   */
  default boolean supportsDirectoryListing(String prefix) {
    return false;
  }

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
