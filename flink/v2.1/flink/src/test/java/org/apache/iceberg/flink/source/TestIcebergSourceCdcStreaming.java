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
package org.apache.iceberg.flink.source;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Test class for CDC (Change Data Capture) streaming reads using the CHANGELOG streaming read mode.
 *
 * <p>This test verifies basic CDC functionality including StreamingReadMode enum values.
 */
public class TestIcebergSourceCdcStreaming {

  @Test
  public void testStreamingReadModeValues() {
    // Test that StreamingReadMode enum has expected values
    assertThat(StreamingReadMode.values()).hasSize(2);
    assertThat(StreamingReadMode.valueOf("APPEND_ONLY")).isEqualTo(StreamingReadMode.APPEND_ONLY);
    assertThat(StreamingReadMode.valueOf("CHANGELOG")).isEqualTo(StreamingReadMode.CHANGELOG);
  }

  @Test
  public void testStreamingReadModeOrdinal() {
    // Test ordinal values
    assertThat(StreamingReadMode.APPEND_ONLY.ordinal()).isEqualTo(0);
    assertThat(StreamingReadMode.CHANGELOG.ordinal()).isEqualTo(1);
  }

  @Test
  public void testStreamingReadModeName() {
    // Test name values
    assertThat(StreamingReadMode.APPEND_ONLY.name()).isEqualTo("APPEND_ONLY");
    assertThat(StreamingReadMode.CHANGELOG.name()).isEqualTo("CHANGELOG");
  }
}
