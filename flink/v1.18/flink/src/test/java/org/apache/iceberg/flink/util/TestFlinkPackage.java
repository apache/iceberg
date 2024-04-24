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
package org.apache.iceberg.flink.util;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class TestFlinkPackage {

  /** This unit test would need to be adjusted as new Flink version is supported. */
  @Test
  public void testVersion() {
    assertThat(FlinkPackage.version()).isEqualTo("1.18.1");
  }

  @Test
  public void testDefaultVersion() {
    // It's difficult to reproduce a reflection error in a unit test, so we just inject a mocked
    // fault to test the default logic

    // First make sure we're not caching a version result from a previous test
    FlinkPackage.setVersion(null);
    try (MockedStatic<FlinkPackage> mockedStatic = Mockito.mockStatic(FlinkPackage.class)) {
      mockedStatic.when(FlinkPackage::versionFromJar).thenThrow(RuntimeException.class);
      mockedStatic.when(FlinkPackage::version).thenCallRealMethod();
      assertThat(FlinkPackage.version()).isEqualTo(FlinkPackage.FLINK_UNKNOWN_VERSION);
    }
    FlinkPackage.setVersion(null);
    try (MockedStatic<FlinkPackage> mockedStatic = Mockito.mockStatic(FlinkPackage.class)) {
      mockedStatic.when(FlinkPackage::versionFromJar).thenReturn(null);
      mockedStatic.when(FlinkPackage::version).thenCallRealMethod();
      FlinkPackage.setVersion(null);
      assertThat(FlinkPackage.version()).isEqualTo(FlinkPackage.FLINK_UNKNOWN_VERSION);
    }
  }
}
