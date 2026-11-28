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

/** A page of files and subdirectories in a directory listing. */
public interface PrefixListingPage {

  /**
   * Returns the files directly below the listed directory prefix.
   *
   * @return immediate files
   */
  Iterable<FileInfo> files();

  /**
   * Returns subdirectory prefixes suitable for a subsequent directory listing.
   *
   * @return immediate subdirectory locations ending in {@code /}
   */
  Iterable<String> subPrefixes();

  /**
   * Creates a page from files and subdirectory prefixes.
   *
   * @param files immediate files
   * @param subPrefixes immediate subdirectory locations ending in {@code /}
   * @return a listing page
   */
  static PrefixListingPage of(Iterable<FileInfo> files, Iterable<String> subPrefixes) {
    return new PrefixListingPage() {
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
