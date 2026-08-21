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
   * Lists files and common prefixes under a prefix, grouped by a delimiter.
   *
   * <p>A file is returned in {@link PrefixListing#files()} when the part of its location after
   * {@code prefix} does not contain {@code delimiter}. When the remaining part contains the
   * delimiter, the file is not returned directly. Instead, {@link PrefixListing#subPrefixes()}
   * contains the common prefix through the first occurrence of the delimiter. Common prefixes are
   * unique, include the delimiter, and are suitable for use in a subsequent listing operation.
   *
   * <p>Implementations can restrict the supported delimiters. Callers must use {@link
   * #supportsPrefixListingWithDelimiter(String, String)} before calling this method.
   *
   * @param prefix prefix to list
   * @param delimiter non-empty delimiter used to group matching locations
   * @return files and common prefixes directly below the prefix
   * @throws UnsupportedOperationException if prefix listing with the delimiter is not supported
   */
  default PrefixListing listPrefix(String prefix, String delimiter) {
    throw new UnsupportedOperationException(
        String.format("Prefix listing with delimiter '%s' is not supported", delimiter));
  }

  /**
   * Returns whether this implementation supports prefix listing with the given delimiter.
   *
   * @param prefix prefix to list
   * @param delimiter non-empty delimiter used to group matching locations
   * @return {@code true} if prefix listing with the delimiter is supported
   */
  default boolean supportsPrefixListingWithDelimiter(String prefix, String delimiter) {
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
