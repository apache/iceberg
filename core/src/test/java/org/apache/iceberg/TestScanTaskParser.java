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
package org.apache.iceberg;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestScanTaskParser {
  @Test
  public void nullCheck() {
    assertThatThrownBy(() -> ScanTaskParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid scan task: null");

    assertThatThrownBy(() -> ScanTaskParser.fromJson(null, true))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid JSON string for scan task: null");
  }

  @Test
  public void invalidTaskType() {
    String jsonStr = "{\"task-type\":\"junk\"}";
    assertThatThrownBy(() -> ScanTaskParser.fromJson(jsonStr, true))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Unknown task type: junk");
  }

  @Test
  public void unsupportedTask() {
    FileScanTask mockTask = Mockito.mock(FileScanTask.class);
    assertThatThrownBy(() -> ScanTaskParser.toJson(mockTask))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining(
            "Unsupported task type: org.apache.iceberg.FileScanTask$MockitoMock$");
  }
}
