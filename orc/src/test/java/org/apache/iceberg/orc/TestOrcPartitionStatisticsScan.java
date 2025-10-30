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
package org.apache.iceberg.orc;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.iceberg.FileFormat;
import org.apache.iceberg.TestBasePartitionStatisticsScan;

public class TestOrcPartitionStatisticsScan extends TestBasePartitionStatisticsScan {
  @Override
  public FileFormat format() {
    return FileFormat.ORC;
  }

  @Override
  public void testScanPartitionStatsForCurrentSnapshot() throws Exception {
    assertThatThrownBy(super::testScanPartitionStatsForCurrentSnapshot)
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Cannot write using unregistered internal data format: ORC");
  }

  @Override
  public void testScanPartitionStatsForOlderSnapshot() throws Exception {
    assertThatThrownBy(super::testScanPartitionStatsForOlderSnapshot)
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Cannot write using unregistered internal data format: ORC");
  }

  @Override
  public void testReadingStatsWithInvalidSchema() throws Exception {
    assertThatThrownBy(super::testReadingStatsWithInvalidSchema)
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Cannot write using unregistered internal data format: ORC");
  }

  @Override
  public void testV2toV3SchemaEvolution() throws Exception {
    assertThatThrownBy(super::testV2toV3SchemaEvolution)
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Cannot write using unregistered internal data format: ORC");
  }
}
