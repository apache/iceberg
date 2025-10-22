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

import java.io.IOException;
import java.util.List;
import java.util.Set;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableSummary;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.hadoop.util.Sets;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.HadoopCatalogExtension;
import org.apache.iceberg.flink.sink.DeltaManifests;
import org.apache.iceberg.flink.sink.DeltaManifestsSerializer;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class TestDynamicWriteResultAggregator {

  @RegisterExtension
  static final HadoopCatalogExtension CATALOG_EXTENSION = new HadoopCatalogExtension("db", "table");

  final int cacheMaximumSize = 1;

  private static final DataFile DATA_FILE =
      DataFiles.builder(PartitionSpec.unpartitioned())
          .withPath("/path/to/data-1.parquet")
          .withFileSizeInBytes(100)
          .withRecordCount(1)
          .build();

  @Test
  void testAggregator() throws Exception {
    CATALOG_EXTENSION.catalog().createTable(TableIdentifier.of("table"), new Schema());
    CATALOG_EXTENSION.catalog().createTable(TableIdentifier.of("table2"), new Schema());

    DynamicWriteResultAggregator aggregator =
        new DynamicWriteResultAggregator(CATALOG_EXTENSION.catalogLoader(), cacheMaximumSize);
    try (OneInputStreamOperatorTestHarness<
            CommittableMessage<DynamicWriteResult>, CommittableMessage<DynamicCommittable>>
        testHarness = new OneInputStreamOperatorTestHarness<>(aggregator)) {
      testHarness.open();

      WriteTarget writeTarget1 = new WriteTarget("table", "branch", 42, 0, true, Sets.newHashSet());
      DynamicWriteResult dynamicWriteResult1 =
          new DynamicWriteResult(writeTarget1, WriteResult.builder().build());
      WriteTarget writeTarget2 =
          new WriteTarget("table2", "branch", 42, 0, true, Sets.newHashSet(1, 2));
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

  @Test
  void testPreventOutputFileFactoryCacheEvictionDuringFlush() throws Exception {
    CATALOG_EXTENSION.catalog().createTable(TableIdentifier.of("table"), new Schema());

    // Disable caching of ManifestOutputFileFactory.
    final int zeroCacheSize = 0;
    DynamicWriteResultAggregator aggregator =
        new DynamicWriteResultAggregator(CATALOG_EXTENSION.catalogLoader(), zeroCacheSize);
    try (OneInputStreamOperatorTestHarness<
            CommittableMessage<DynamicWriteResult>, CommittableMessage<DynamicCommittable>>
        testHarness = new OneInputStreamOperatorTestHarness<>(aggregator)) {
      testHarness.open();

      WriteTarget writeTarget1 =
          new WriteTarget("table", "branch", 42, 0, false, Sets.newHashSet());
      DynamicWriteResult dynamicWriteResult1 =
          new DynamicWriteResult(
              writeTarget1, WriteResult.builder().addDataFiles(DATA_FILE).build());

      // Different WriteTarget
      WriteTarget writeTarget2 =
          new WriteTarget("table", "branch2", 23, 0, true, Sets.newHashSet());
      DynamicWriteResult dynamicWriteResult2 =
          new DynamicWriteResult(
              writeTarget2, WriteResult.builder().addDataFiles(DATA_FILE).build());

      CommittableWithLineage<DynamicWriteResult> committable1 =
          new CommittableWithLineage<>(dynamicWriteResult1, 0, 0);
      testHarness.processElement(new StreamRecord<>(committable1));

      CommittableWithLineage<DynamicWriteResult> committable2 =
          new CommittableWithLineage<>(dynamicWriteResult2, 0, 0);
      testHarness.processElement(new StreamRecord<>(committable2));

      assertThat(testHarness.getOutput()).isEmpty();

      testHarness.prepareSnapshotPreBarrier(1L);
      List<StreamRecord<CommittableMessage<DynamicCommittable>>> output =
          Lists.newArrayList(testHarness.getRecordOutput().iterator());

      assertThat(testHarness.getOutput()).hasSize(3);
      assertThat(output.get(0).getValue()).isInstanceOf(CommittableSummary.class);
      assertThat(output.get(1).getValue()).isInstanceOf(CommittableWithLineage.class);
      assertThat(output.get(2).getValue()).isInstanceOf(CommittableWithLineage.class);

      // There should be two unique file paths, despite the cache being disabled.
      Set<String> manifestPaths = getManifestPaths(output);
      assertThat(manifestPaths).hasSize(2);
    }
  }

  private static Set<String> getManifestPaths(
      List<StreamRecord<CommittableMessage<DynamicCommittable>>> messages) throws IOException {
    Set<String> manifestPaths = Sets.newHashSet();

    for (StreamRecord<CommittableMessage<DynamicCommittable>> record : messages) {
      CommittableMessage<DynamicCommittable> message = record.getValue();
      if (message instanceof CommittableWithLineage) {
        DeltaManifests deltaManifests =
            SimpleVersionedSerialization.readVersionAndDeSerialize(
                DeltaManifestsSerializer.INSTANCE,
                (((CommittableWithLineage<DynamicCommittable>) message).getCommittable())
                    .manifest());
        deltaManifests.manifests().forEach(manifest -> manifestPaths.add(manifest.path()));
      }
    }

    return manifestPaths;
  }
}
