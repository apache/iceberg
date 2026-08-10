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
 * Result of a shallow (single-level) prefix listing, separating files found immediately under the
 * prefix from sub-prefixes that would be expanded further.
 *
 * <p>For key/value object stores this maps directly to a delimited listing where {@link #files()}
 * corresponds to entries returned as objects and {@link #subPrefixes()} corresponds to entries
 * returned as common prefixes. For hierarchical file systems, {@link #files()} contains regular
 * files found directly under the prefix and {@link #subPrefixes()} contains sub-directory
 * locations.
 */
public interface PrefixListing {

  /** Files found immediately under the listed prefix. */
  Iterable<FileInfo> files();

  /** Sub-prefixes one level below the listed prefix. Each is a location suitable for re-listing. */
  Iterable<String> subPrefixes();

  /** Create a {@link PrefixListing} from the given iterables. */
  static PrefixListing of(Iterable<FileInfo> files, Iterable<String> subPrefixes) {
    return new PrefixListing() {
      @Override
      public Iterable<FileInfo> files() {
        return files;
      }

      @Override
      public Iterable<String> subPrefixes() {
        return subPrefixes;
      }
    };
  }
}
