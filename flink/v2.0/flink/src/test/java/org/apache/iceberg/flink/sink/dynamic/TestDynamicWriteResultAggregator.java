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
package org.apache.iceberg.flink.sink.dynamic;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.HadoopCatalogExtension;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class TestDynamicWriteResultAggregator {

  @RegisterExtension
  static final HadoopCatalogExtension CATALOG_EXTENSION = new HadoopCatalogExtension("db", "table");

  @Test
  void testAggregator() throws Exception {
    CATALOG_EXTENSION.catalog().createTable(TableIdentifier.of("table"), new Schema());
    CATALOG_EXTENSION.catalog().createTable(TableIdentifier.of("table2"), new Schema());

    DynamicWriteResultAggregator aggregator =
        new DynamicWriteResultAggregator(CATALOG_EXTENSION.catalogLoader());
    try (OneInputStreamOperatorTestHarness<
            CommittableMessage<DynamicWriteResult>, CommittableMessage<DynamicCommittable>>
        testHarness = new OneInputStreamOperatorTestHarness<>(aggregator)) {
      testHarness.open();

      WriteTarget writeTarget1 =
          new WriteTarget("table", "branch", 42, 0, true, Lists.newArrayList());
      DynamicWriteResult dynamicWriteResult1 =
          new DynamicWriteResult(writeTarget1, WriteResult.builder().build());
      WriteTarget writeTarget2 =
          new WriteTarget("table2", "branch", 42, 0, true, Lists.newArrayList(1, 2));
      DynamicWriteResult dynamicWriteResult2 =
          new DynamicWriteResult(writeTarget2, WriteResult.builder().build());

      CommittableWithLineage<DynamicWriteResult> committable1 =
          new CommittableWithLineage<>(dynamicWriteResult1, 0, 0);
      StreamRecord<CommittableMessage<DynamicWriteResult>> record1 =
          new StreamRecord<>(committable1);
      testHarness.processElement(record1);
      CommittableWithLineage<DynamicWriteResult> committable2 =
          new CommittableWithLineage<>(dynamicWriteResult2, 0, 0);
      StreamRecord<CommittableMessage<DynamicWriteResult>> record2 =
          new StreamRecord<>(committable2);
      testHarness.processElement(record2);

      assertThat(testHarness.getOutput()).isEmpty();

      testHarness.prepareSnapshotPreBarrier(1L);
      // Contains a CommittableSummary + DynamicCommittable
      assertThat(testHarness.getRecordOutput()).hasSize(3);

      testHarness.prepareSnapshotPreBarrier(2L);
      // Only contains a CommittableSummary
      assertThat(testHarness.getRecordOutput()).hasSize(4);
    }
  }
}
