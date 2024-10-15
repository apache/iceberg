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
package org.apache.iceberg.flink.sink;

import static org.apache.iceberg.flink.TestFixtures.DATABASE;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.flink.HadoopCatalogExtension;
import org.apache.iceberg.flink.MiniFlinkClusterExtension;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

@ExtendWith(ParameterizedTestExtension.class)
public class TestFlinkIcebergSinkV2Branch extends TestFlinkIcebergSinkV2Base {
  @RegisterExtension
  static final HadoopCatalogExtension CATALOG_EXTENSION =
      new HadoopCatalogExtension(DATABASE, TestFixtures.TABLE);

  @Parameter(index = 0)
  private String branch;

  @Override
  protected int getParallelism() {
    return 1;
  }

  @Override
  protected boolean isPartitioned() {
    return false;
  }

  @Override
  protected String getWriteDistributionMode() {
    return null;
  }

  @Parameters(name = "branch = {0}")
  public static Object[][] parameters() {
    return new Object[][] {new Object[] {"main"}, new Object[] {"testBranch"}};
  }

  @BeforeEach
  public void before() throws IOException {
    table =
        CATALOG_EXTENSION
            .catalog()
            .createTable(
                TestFixtures.TABLE_IDENTIFIER,
                SimpleDataUtil.SCHEMA,
                PartitionSpec.unpartitioned(),
                ImmutableMap.of(
                    TableProperties.DEFAULT_FILE_FORMAT,
                    FileFormat.AVRO.name(),
                    TableProperties.FORMAT_VERSION,
                    "2"));

    env =
        StreamExecutionEnvironment.getExecutionEnvironment(
                MiniFlinkClusterExtension.DISABLE_CLASSLOADER_CHECK_CONFIG)
            .enableCheckpointing(100);

    tableLoader = CATALOG_EXTENSION.tableLoader();
  }

  @TestTemplate
  public void testChangeLogOnIdKey() throws Exception {
    testChangeLogOnIdKey(branch);
    verifyOtherBranchUnmodified();
  }

  @TestTemplate
  public void testChangeLogOnDataKey() throws Exception {
    testChangeLogOnDataKey(branch);
    verifyOtherBranchUnmodified();
  }

  @TestTemplate
  public void testChangeLogOnIdDataKey() throws Exception {
    testChangeLogOnIdDataKey(branch);
    verifyOtherBranchUnmodified();
  }

  @TestTemplate
  public void testUpsertOnIdKey() throws Exception {
    testUpsertOnIdKey(branch);
    verifyOtherBranchUnmodified();
  }

  @TestTemplate
  public void testUpsertOnDataKey() throws Exception {
    testUpsertOnDataKey(branch);
    verifyOtherBranchUnmodified();
  }

  @TestTemplate
  public void testUpsertOnIdDataKey() throws Exception {
    testUpsertOnIdDataKey(branch);
    verifyOtherBranchUnmodified();
  }

  private void verifyOtherBranchUnmodified() {
    String otherBranch =
        branch.equals(SnapshotRef.MAIN_BRANCH) ? "test-branch" : SnapshotRef.MAIN_BRANCH;
    if (otherBranch.equals(SnapshotRef.MAIN_BRANCH)) {
      assertThat(table.currentSnapshot()).isNull();
    }

    assertThat(table.snapshot(otherBranch)).isNull();
  }
}
