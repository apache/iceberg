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
package org.apache.iceberg.flink.maintenance.operator;

import static org.apache.iceberg.flink.maintenance.operator.RewriteUtil.planDataFileRewrite;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.flink.maintenance.operator.DataFileRewritePlanner.PlannedGroup;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.SerializableSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** The planned groups are sent from the planner to the runner, serialized by Flink with Kryo. */
public class TestPlannedGroupSerialization extends OperatorTestBase {

  @TempDir private Path lambdaWarehouse;

  @Test
  void testRoundTrip() throws Exception {
    Table table = createTable();
    insert(table, 1, "a");
    insert(table, 2, "b");

    PlannedGroup original = planDataFileRewrite(tableLoader()).get(0);

    assertSameGroup(original, roundTrip(original));
  }

  /**
   * The failure of the planner with S3FileIO: "KryoException: can't get field offset on a hidden
   * class: ... S3FileIO$$Lambda ... arg$1", logged as "Failed to plan data file rewrite groups".
   */
  @Test
  void testRoundTripWhenTheFileIOHoldsALambda() throws Exception {
    CatalogLoader catalogLoader =
        CatalogLoader.hadoop(
            "lambda-io",
            new Configuration(),
            ImmutableMap.of(
                CatalogProperties.WAREHOUSE_LOCATION,
                lambdaWarehouse.toString(),
                CatalogProperties.FILE_IO_IMPL,
                LambdaHoldingFileIO.class.getName()));
    Table table =
        catalogLoader
            .loadCatalog()
            .createTable(
                TestFixtures.TABLE_IDENTIFIER,
                SimpleDataUtil.SCHEMA,
                PartitionSpec.unpartitioned());
    assertThat(table.io()).isInstanceOf(LambdaHoldingFileIO.class);
    insert(table, 1, "a");
    insert(table, 2, "b");

    TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, TestFixtures.TABLE_IDENTIFIER);
    PlannedGroup original = planDataFileRewrite(tableLoader).get(0);
    PlannedGroup received = roundTrip(original);

    assertSameGroup(original, received);
    assertThat(received.table().io()).isInstanceOf(LambdaHoldingFileIO.class);
  }

  private static PlannedGroup roundTrip(PlannedGroup original) throws Exception {
    TypeSerializer<PlannedGroup> serializer =
        TypeInformation.of(PlannedGroup.class).createSerializer(new SerializerConfigImpl());
    DataOutputSerializer out = new DataOutputSerializer(1024);
    serializer.serialize(original, out);
    return serializer.deserialize(new DataInputDeserializer(out.getCopyOfBuffer()));
  }

  private static void assertSameGroup(PlannedGroup original, PlannedGroup copy) {
    assertThat(locations(copy)).isEqualTo(locations(original));
    assertThat(copy.groupsPerCommit()).isEqualTo(original.groupsPerCommit());
    assertThat(copy.branch()).isEqualTo(original.branch());
    assertThat(copy.table().currentSnapshot().snapshotId())
        .isEqualTo(original.table().currentSnapshot().snapshotId());
  }

  private static List<String> locations(PlannedGroup group) {
    return group.group().rewrittenFiles().stream()
        .map(file -> file.location())
        .sorted()
        .collect(Collectors.toList());
  }

  /**
   * A FileIO that holds a lambda in a field, as {@code S3FileIO} does (it keeps the {@code
   * AwsClientFactory::s3} supplier). A lambda is a hidden class on Java 17, which Kryo cannot
   * serialize.
   */
  public static class LambdaHoldingFileIO extends HadoopFileIO {
    private final SerializableSupplier<String> supplier;

    public LambdaHoldingFileIO() {
      String captured = "client";
      this.supplier = () -> captured;
    }
  }
}
