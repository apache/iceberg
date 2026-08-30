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
package org.apache.iceberg.util;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class TestCharSequenceUtil {

  @Test
  void unequalPathsReturnsFalseForSameReference() {
    CharSequence path = "s3://bucket/table/data/file.parquet";
    assertThat(CharSequenceUtil.unequalPaths(path, path)).isFalse();
  }

  @Test
  void unequalPathsReturnsFalseForEqualContentInDifferentInstances() {
    String path = "s3://bucket/table/data/file.parquet";
    // a distinct String instance and a different CharSequence implementation with equal content
    assertThat(CharSequenceUtil.unequalPaths(path, new String(path.toCharArray()))).isFalse();
    assertThat(CharSequenceUtil.unequalPaths(path, new StringBuilder(path))).isFalse();
  }

  @Test
  void unequalPathsReturnsTrueForDifferentLengths() {
    assertThat(CharSequenceUtil.unequalPaths("data/file.parquet", "data/file.parquet.crc"))
        .isTrue();
  }

  @Test
  void unequalPathsReturnsTrueWhenLastCharacterDiffers() {
    // paths are compared from the end, so a trailing difference is found immediately
    assertThat(CharSequenceUtil.unequalPaths("data/file-1.parquet", "data/file-2.parquet"))
        .isTrue();
  }

  @Test
  void unequalPathsReturnsTrueWhenFirstCharacterDiffers() {
    // forces the scan to walk all the way back to the first character before differing
    assertThat(CharSequenceUtil.unequalPaths("a/data/file.parquet", "b/data/file.parquet"))
        .isTrue();
  }

  @Test
  void unequalPathsReturnsFalseForTwoEmptySequences() {
    assertThat(CharSequenceUtil.unequalPaths("", new StringBuilder())).isFalse();
  }

  @Test
  void unequalPathsReturnsTrueWhenOnlyOneSequenceIsEmpty() {
    assertThat(CharSequenceUtil.unequalPaths("", "data/file.parquet")).isTrue();
  }
}
