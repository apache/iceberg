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

public class TestOrcPartitionStatsHandler extends PartitionStatsHandlerTestBase {

  public FileFormat format() {
    return FileFormat.ORC;
  }

  @Override
  public void testAllDatatypePartitionWriting() throws Exception {
    assertThatThrownBy(super::testAllDatatypePartitionWriting)
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Cannot write using unregistered internal data format: ORC");
  }

  @Override
  public void testOptionalFieldsWriting() throws Exception {
    assertThatThrownBy(super::testOptionalFieldsWriting)
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Cannot write using unregistered internal data format: ORC");
  }

  @Override
  public void testPartitionStats() throws Exception {
    assertThatThrownBy(super::testPartitionStats)
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Cannot write using unregistered internal data format: ORC");
  }

  @Override
  public void testLatestStatsFile() throws Exception {
    assertThatThrownBy(super::testLatestStatsFile)
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Cannot write using unregistered internal data format: ORC");
  }

  @Override
  public void testLatestStatsFileWithBranch() throws Exception {
    assertThatThrownBy(super::testLatestStatsFileWithBranch)
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Cannot write using unregistered internal data format: ORC");
  }

  @Override
  public void testCopyOnWriteDelete() throws Exception {
    assertThatThrownBy(super::testCopyOnWriteDelete)
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Cannot write using unregistered internal data format: ORC");
  }

  @Override
  public void testReadingStatsWithInvalidSchema() {
    assertThatThrownBy(super::testReadingStatsWithInvalidSchema)
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Cannot write using unregistered internal data format: ORC");
  }

  @Override
  public void testFullComputeFallbackWithInvalidStats() {
    assertThatThrownBy(super::testFullComputeFallbackWithInvalidStats)
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Cannot write using unregistered internal data format: ORC");
  }

  @Override
  public void testV2toV3SchemaEvolution() {
    assertThatThrownBy(super::testV2toV3SchemaEvolution)
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Cannot write using unregistered internal data format: ORC");
  }
}
