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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Collections;
import org.junit.jupiter.api.Test;

class TestSupportsPrefixOperations {

  @Test
  void prefixListingContainsPages() {
    FileInfo file = new FileInfo("file:/table/file.parquet", 10L, 20L);
    PrefixListingPage page =
        PrefixListingPage.of(
            Collections.singletonList(file), Collections.singletonList("file:/table/partition/"));
    PrefixListing listing = PrefixListing.of(Collections.singletonList(page));

    assertThat(listing.pages()).containsExactly(page);
    assertThat(page.files()).containsExactly(file);
    assertThat(page.subPrefixes()).containsExactly("file:/table/partition/");
  }

  @Test
  void delimitedListingIsUnsupportedByDefault() {
    SupportsPrefixOperations io = new TestFileIO();

    assertThat(io.supportsPrefixListingWithDelimiter("file:/table/", "/")).isFalse();
    assertThatThrownBy(() -> io.listPrefix("file:/table/", "/"))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Prefix listing with delimiter '/' is not supported");
  }

  private static class TestFileIO implements SupportsPrefixOperations {
    @Override
    public InputFile newInputFile(String path) {
      throw new UnsupportedOperationException();
    }

    @Override
    public OutputFile newOutputFile(String path) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void deleteFile(String path) {}

    @Override
    public Iterable<FileInfo> listPrefix(String prefix) {
      return Collections.emptyList();
    }

    @Override
    public void deletePrefix(String prefix) {}
  }
}
