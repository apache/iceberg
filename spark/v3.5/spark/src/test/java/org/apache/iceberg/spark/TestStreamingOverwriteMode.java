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
package org.apache.iceberg.spark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

public class TestStreamingOverwriteMode {

  @Test
  public void testFromNameFail() {
    assertThat(StreamingOverwriteMode.fromName("fail")).isEqualTo(StreamingOverwriteMode.FAIL);
    assertThat(StreamingOverwriteMode.fromName("FAIL")).isEqualTo(StreamingOverwriteMode.FAIL);
    assertThat(StreamingOverwriteMode.fromName("Fail")).isEqualTo(StreamingOverwriteMode.FAIL);
  }

  @Test
  public void testFromNameSkip() {
    assertThat(StreamingOverwriteMode.fromName("skip")).isEqualTo(StreamingOverwriteMode.SKIP);
    assertThat(StreamingOverwriteMode.fromName("SKIP")).isEqualTo(StreamingOverwriteMode.SKIP);
    assertThat(StreamingOverwriteMode.fromName("Skip")).isEqualTo(StreamingOverwriteMode.SKIP);
  }

  @Test
  public void testFromNameAddedFilesOnly() {
    assertThat(StreamingOverwriteMode.fromName("added-files-only"))
        .isEqualTo(StreamingOverwriteMode.ADDED_FILES_ONLY);
    assertThat(StreamingOverwriteMode.fromName("ADDED-FILES-ONLY"))
        .isEqualTo(StreamingOverwriteMode.ADDED_FILES_ONLY);
    assertThat(StreamingOverwriteMode.fromName("Added-Files-Only"))
        .isEqualTo(StreamingOverwriteMode.ADDED_FILES_ONLY);
  }

  @Test
  public void testFromNameInvalid() {
    assertThatThrownBy(() -> StreamingOverwriteMode.fromName("invalid"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unknown streaming overwrite mode: invalid");
  }

  @Test
  public void testFromNameNull() {
    assertThatThrownBy(() -> StreamingOverwriteMode.fromName(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Mode name is null");
  }

  @Test
  public void testModeName() {
    assertThat(StreamingOverwriteMode.FAIL.modeName()).isEqualTo("fail");
    assertThat(StreamingOverwriteMode.SKIP.modeName()).isEqualTo("skip");
    assertThat(StreamingOverwriteMode.ADDED_FILES_ONLY.modeName()).isEqualTo("added-files-only");
  }
}
