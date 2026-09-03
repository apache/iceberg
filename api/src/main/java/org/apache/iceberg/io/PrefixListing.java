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
 * Result of a delimited prefix listing, returned as an iterable of pages.
 *
 * <p>Pages are retrieved lazily and each page separates files from common prefixes that group files
 * containing the delimiter.
 */
public interface PrefixListing {

  /** Pages in this listing. */
  Iterable<PrefixListingPage> pages();

  /** Create a {@link PrefixListing} from the given pages. */
  static PrefixListing of(Iterable<PrefixListingPage> pages) {
    return new PrefixListing() {
      @Override
      public Iterable<PrefixListingPage> pages() {
        return pages;
      }
    };
  }
}
