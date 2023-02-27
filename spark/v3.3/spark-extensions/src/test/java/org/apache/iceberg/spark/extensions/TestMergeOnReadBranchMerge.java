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

public class TestMergeOnReadBranchMerge extends TestMergeOnReadMerge {

  public TestMergeOnReadBranchMerge(
      String catalogName,
      String implementation,
      Map<String, String> config,
      String fileFormat,
      boolean vectorized,
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
        "orc",
        true,
        WRITE_DISTRIBUTION_MODE_NONE
      },
    };
  }

  @Test
  public void testMergeConditionSplitIntoTargetPredicateAndJoinCondition() {
    testMergeConditionSplitIntoTargetPredicateAndJoinCondition(TEST_BRANCH);
  }

  @Test
  public void testMergeWithStaticPredicatePushDown() {
    testMergeWithStaticPredicatePushDown(TEST_BRANCH);
  }

  @Test
  public void testMergeIntoEmptyTargetInsertAllNonMatchingRows() {
    testMergeIntoEmptyTargetInsertAllNonMatchingRows(TEST_BRANCH);
  }

  @Test
  public void testMergeIntoEmptyTargetInsertOnlyMatchingRows() {
    testMergeIntoEmptyTargetInsertOnlyMatchingRows(TEST_BRANCH);
  }

  @Test
  public void testMergeWithOnlyUpdateClause() {
    testMergeWithOnlyUpdateClause(TEST_BRANCH);
  }

  @Test
  public void testMergeWithOnlyUpdateClauseAndNullValues() {
    testMergeWithOnlyUpdateClauseAndNullValues(TEST_BRANCH);
  }

  @Test
  public void testMergeWithOnlyDeleteClause() {
    testMergeWithOnlyDeleteClause(TEST_BRANCH);
  }

  @Test
  public void testMergeWithAllCauses() {
    testMergeWithAllCauses(TEST_BRANCH);
  }

  @Test
  public void testMergeWithAllCausesWithExplicitColumnSpecification() {
    testMergeWithAllCausesWithExplicitColumnSpecification(TEST_BRANCH);
  }

  @Test
  public void testMergeWithSourceCTE() {
    testMergeWithSourceCTE(TEST_BRANCH);
  }

  @Test
  public void testMergeWithSourceFromSetOps() {
    testMergeWithSourceFromSetOps(TEST_BRANCH);
  }

  @Test
  public void testMergeWithMultipleUpdatesForTargetRowSmallTargetLargeSource() {
    testMergeWithMultipleUpdatesForTargetRowSmallTargetLargeSource(TEST_BRANCH);
  }

  @Test
  public void
      testMergeWithMultipleUpdatesForTargetRowSmallTargetLargeSourceEnabledHashShuffleJoin() {
    testMergeWithMultipleUpdatesForTargetRowSmallTargetLargeSourceEnabledHashShuffleJoin(
        TEST_BRANCH);
  }

  @Test
  public void testMergeWithMultipleUpdatesForTargetRowSmallTargetLargeSourceNoEqualityCondition() {
    testMergeWithMultipleUpdatesForTargetRowSmallTargetLargeSourceEnabledHashShuffleJoin(
        TEST_BRANCH);
  }

  @Test
  public void testMergeWithMultipleUpdatesForTargetRowSmallTargetLargeSourceNoNotMatchedActions() {
    testMergeWithMultipleUpdatesForTargetRowSmallTargetLargeSourceNoNotMatchedActions(TEST_BRANCH);
  }

  @Test
  public void
      testMergeWithMultipleUpdatesForTargetRowSmallTargetLargeSourceNoNotMatchedActionsNoEqualityCondition() {
    testMergeWithMultipleUpdatesForTargetRowSmallTargetLargeSourceNoNotMatchedActionsNoEqualityCondition(
        TEST_BRANCH);
  }

  @Test
  public void testMergeWithMultipleUpdatesForTargetRow() {
    testMergeWithMultipleUpdatesForTargetRow(TEST_BRANCH);
  }

  @Test
  public void testMergeWithUnconditionalDelete() {
    testMergeWithUnconditionalDelete(TEST_BRANCH);
  }

  @Test
  public void testMergeWithSingleConditionalDelete() {
    testMergeWithSingleConditionalDelete(TEST_BRANCH);
  }

  @Test
  public void testMergeWithIdentityTransform() {
    testMergeWithIdentityTransform(TEST_BRANCH);
  }

  @Test
  public void testMergeWithDaysTransform() {
    testMergeWithDaysTransform(TEST_BRANCH);
  }

  @Test
  public void testMergeWithBucketTransform() {
    testMergeWithBucketTransform(TEST_BRANCH);
  }

  @Test
  public void testMergeWithTruncateTransform() {
    testMergeWithTruncateTransform(TEST_BRANCH);
  }

  @Test
  public void testMergeIntoPartitionedAndOrderedTable() {
    testMergeIntoPartitionedAndOrderedTable(TEST_BRANCH);
  }

  @Test
  public void testSelfMerge() {
    testSelfMerge(TEST_BRANCH);
  }

  @Test
  public void testSelfMergeWithCaching() {
    testSelfMergeWithCaching(TEST_BRANCH);
  }

  @Test
  public void testMergeWithSourceAsSelfSubquery() {
    testMergeWithSourceAsSelfSubquery(TEST_BRANCH);
  }

  @Test
  public synchronized void testMergeWithSerializableIsolation() throws InterruptedException {
    testMergeWithSerializableIsolation(TEST_BRANCH);
  }

  @Test
  public synchronized void testMergeWithSnapshotIsolation()
      throws InterruptedException, ExecutionException {
    testMergeWithSnapshotIsolation(TEST_BRANCH);
  }

  @Test
  public void testMergeWithExtraColumnsInSource() {
    testMergeWithExtraColumnsInSource(TEST_BRANCH);
  }

  @Test
  public void testMergeWithNullsInTargetAndSource() {
    testMergeWithNullsInTargetAndSource(TEST_BRANCH);
  }

  @Test
  public void testMergeWithNullSafeEquals() {
    testMergeWithNullSafeEquals(TEST_BRANCH);
  }

  @Test
  public void testMergeWithNullCondition() {
    testMergeWithNullCondition(TEST_BRANCH);
  }

  @Test
  public void testMergeWithNullActionConditions() {
    testMergeWithNullActionConditions(TEST_BRANCH);
  }

  @Test
  public void testMergeWithMultipleMatchingActions() {
    testMergeWithMultipleMatchingActions(TEST_BRANCH);
  }

  @Test
  public void testMergeWithMultipleRowGroupsParquet() throws NoSuchTableException {
    testMergeWithMultipleRowGroupsParquet(TEST_BRANCH);
  }

  @Test
  public void testMergeInsertOnly() {
    testMergeInsertOnly(TEST_BRANCH);
  }

  @Test
  public void testMergeInsertOnlyWithCondition() {
    testMergeInsertOnlyWithCondition(TEST_BRANCH);
  }

  @Test
  public void testMergeAlignsUpdateAndInsertActions() {
    testMergeAlignsUpdateAndInsertActions(TEST_BRANCH);
  }

  @Test
  public void testMergeMixedCaseAlignsUpdateAndInsertActions() {
    testMergeMixedCaseAlignsUpdateAndInsertActions(TEST_BRANCH);
  }

  @Test
  public void testMergeUpdatesNestedStructFields() {
    testMergeUpdatesNestedStructFields(TEST_BRANCH);
  }

  @Test
  public void testMergeWithInferredCasts() {
    testMergeWithInferredCasts(TEST_BRANCH);
  }

  @Test
  public void testMergeModifiesNullStruct() {
    testMergeModifiesNullStruct(TEST_BRANCH);
  }

  @Test
  public void testMergeRefreshesRelationCache() {
    testMergeRefreshesRelationCache(TEST_BRANCH);
  }

  @Test
  public void testMergeWithMultipleNotMatchedActions() {
    testMergeWithMultipleNotMatchedActions(TEST_BRANCH);
  }

  @Test
  public void testMergeWithMultipleConditionalNotMatchedActions() {
    testMergeWithMultipleConditionalNotMatchedActions(TEST_BRANCH);
  }

  @Test
  public void testMergeResolvesColumnsByName() {
    testMergeResolvesColumnsByName(TEST_BRANCH);
  }

  @Test
  public void testMergeShouldResolveWhenThereAreNoUnresolvedExpressionsOrColumns() {
    testMergeShouldResolveWhenThereAreNoUnresolvedExpressionsOrColumns(TEST_BRANCH);
  }

  @Test
  public void testMergeWithTableWithNonNullableColumn() {
    testMergeWithTableWithNonNullableColumn(TEST_BRANCH);
  }

  @Test
  public void testMergeWithNonExistingColumns() {
    testMergeWithNonExistingColumns(TEST_BRANCH);
  }

  @Test
  public void testMergeWithInvalidColumnsInInsert() {
    testMergeWithInvalidColumnsInInsert(TEST_BRANCH);
  }

  @Test
  public void testMergeWithInvalidUpdates() {
    testMergeWithInvalidUpdates(TEST_BRANCH);
  }

  @Test
  public void testMergeWithConflictingUpdates() {
    testMergeWithConflictingUpdates(TEST_BRANCH);
  }

  @Test
  public void testMergeWithInvalidAssignments() {
    testMergeWithInvalidAssignments(TEST_BRANCH);
  }

  @Test
  public void testMergeWithNonDeterministicConditions() {
    testMergeWithNonDeterministicConditions(TEST_BRANCH);
  }

  @Test
  public void testMergeWithAggregateExpressions() {
    testMergeWithAggregateExpressions(TEST_BRANCH);
  }

  @Test
  public void testMergeWithSubqueriesInConditions() {
    testMergeWithSubqueriesInConditions(TEST_BRANCH);
  }

  @Test
  public void testMergeWithTargetColumnsInInsertConditions() {
    testMergeWithTargetColumnsInInsertConditions(TEST_BRANCH);
  }

  @Test
  public void testMergeWithNonIcebergTargetTableNotSupported() {
    testMergeWithNonIcebergTargetTableNotSupported(TEST_BRANCH);
  }

  /**
   * Tests a merge where both the source and target are evaluated to be partitioned by
   * SingePartition at planning time but DynamicFileFilterExec will return an empty target.
   */
  @Test
  public void testMergeSinglePartitionPartitioning() {
    testMergeSinglePartitionPartitioning(TEST_BRANCH);
  }

  @Test
  public void testMergeEmptyTable() {
    testMergeEmptyTable(TEST_BRANCH);
  }
}
