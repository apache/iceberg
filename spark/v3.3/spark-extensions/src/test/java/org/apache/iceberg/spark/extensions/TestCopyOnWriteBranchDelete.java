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
package org.apache.iceberg.spark.extensions;

import static org.apache.iceberg.TableProperties.WRITE_DISTRIBUTION_MODE_NONE;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.junit.Test;
import org.junit.runners.Parameterized;

public class TestCopyOnWriteBranchDelete extends TestCopyOnWriteDelete {

  static String TEST_BRANCH = "test_branch";

  public TestCopyOnWriteBranchDelete(
      String catalogName,
      String implementation,
      Map<String, String> config,
      String fileFormat,
      Boolean vectorized,
      String distributionMode) {
    super(catalogName, implementation, config, fileFormat, vectorized, distributionMode);
  }

  @Parameterized.Parameters(
      name =
          "catalogName = {0}, implementation = {1}, config = {2},"
              + " format = {3}, vectorized = {4}, distributionMode = {5}")
  public static Object[][] parameters() {
    return new Object[][] {
      {
        "testhive",
        SparkCatalog.class.getName(),
        ImmutableMap.of(
            "type", "hive",
            "default-namespace", "default"),
        "parquet",
        true,
        WRITE_DISTRIBUTION_MODE_NONE
      },
    };
  }

  @Test
  public void testDeleteWithoutScanningTable() throws Exception {
    testDeleteWithoutScanningTable(TEST_BRANCH);
  }

  @Test
  public void testDeleteFileThenMetadataDelete() throws Exception {
    testDeleteFileThenMetadataDelete(TEST_BRANCH);
  }

  @Test
  public void testDeleteWithFalseCondition() {
    testDeleteWithFalseCondition(TEST_BRANCH);
  }

  @Test
  public void testDeleteFromEmptyTable() {
    testDeleteFromEmptyTable(TEST_BRANCH);
  }

  @Test
  public void testExplain() {
    testExplain(TEST_BRANCH);
  }

  @Test
  public void testDeleteWithAlias() {
    testDeleteWithAlias(TEST_BRANCH);
  }

  @Test
  public void testDeleteWithDynamicFileFiltering() throws NoSuchTableException {
    testDeleteWithDynamicFileFiltering(TEST_BRANCH);
  }

  @Test
  public void testDeleteNonExistingRecords() {
    testDeleteNonExistingRecords(TEST_BRANCH);
  }

  @Test
  public void testDeleteWithoutCondition() {
    testDeleteWithoutCondition(TEST_BRANCH);
  }

  @Test
  public void testDeleteUsingMetadataWithComplexCondition() {
    testDeleteUsingMetadataWithComplexCondition(TEST_BRANCH);
  }

  @Test
  public void testDeleteWithArbitraryPartitionPredicates() {
    testDeleteWithArbitraryPartitionPredicates(TEST_BRANCH);
  }

  @Test
  public void testDeleteWithNonDeterministicCondition() {
    testDeleteWithNonDeterministicCondition(TEST_BRANCH);
  }

  @Test
  public void testDeleteWithFoldableConditions() {
    testDeleteWithFoldableConditions(TEST_BRANCH);
  }

  @Test
  public void testDeleteWithNullConditions() {
    testDeleteWithNullConditions(TEST_BRANCH);
  }

  @Test
  public void testDeleteWithInAndNotInConditions() {
    testDeleteWithInAndNotInConditions(TEST_BRANCH);
  }

  @Test
  public void testDeleteWithMultipleRowGroupsParquet() throws NoSuchTableException {
    testDeleteWithMultipleRowGroupsParquet(TEST_BRANCH);
  }

  @Test
  public void testDeleteWithConditionOnNestedColumn() {
    testDeleteWithConditionOnNestedColumn(TEST_BRANCH);
  }

  @Test
  public void testDeleteWithInSubquery() throws NoSuchTableException {
    testDeleteWithInSubquery(TEST_BRANCH);
  }

  @Test
  public void testDeleteWithMultiColumnInSubquery() throws NoSuchTableException {
    testDeleteWithNotInSubquery(TEST_BRANCH);
  }

  @Test
  public void testDeleteWithNotInSubquery() throws NoSuchTableException {
    testDeleteWithNotInSubquery(TEST_BRANCH);
  }

  @Test
  public void testDeleteOnNonIcebergTableNotSupported() {
    testDeleteOnNonIcebergTableNotSupported(TEST_BRANCH);
  }

  @Test
  public void testDeleteWithExistSubquery() throws NoSuchTableException {
    testDeleteWithExistSubquery(TEST_BRANCH);
  }

  @Test
  public void testDeleteWithNotExistsSubquery() throws NoSuchTableException {
    testDeleteWithNotExistsSubquery(TEST_BRANCH);
  }

  @Test
  public void testDeleteWithScalarSubquery() throws NoSuchTableException {
    testDeleteWithScalarSubquery(TEST_BRANCH);
  }

  @Test
  public synchronized void testDeleteWithSerializableIsolation() throws InterruptedException {
    testDeleteWithSerializableIsolation(TEST_BRANCH);
  }

  @Test
  public synchronized void testDeleteWithSnapshotIsolation()
      throws InterruptedException, ExecutionException {
    testDeleteWithSnapshotIsolation(TEST_BRANCH);
  }

  @Test
  public void testDeleteRefreshesRelationCache() throws NoSuchTableException {
    testDeleteRefreshesRelationCache(TEST_BRANCH);
  }

  @Test
  public void testDeleteWithMultipleSpecs() {
    testDeleteWithMultipleSpecs(TEST_BRANCH);
  }
}
