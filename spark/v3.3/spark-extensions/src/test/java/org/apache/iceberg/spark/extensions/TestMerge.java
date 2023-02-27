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

import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.iceberg.SnapshotRef;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

public abstract class TestMerge extends TestMergeBase {

  public TestMerge(
      String catalogName,
      String implementation,
      Map<String, String> config,
      String fileFormat,
      boolean vectorized,
      String distributionMode) {
    super(catalogName, implementation, config, fileFormat, vectorized, distributionMode);
  }

  @BeforeClass
  public static void setupSparkConf() {
    spark.conf().set("spark.sql.shuffle.partitions", "4");
  }

  @After
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
    sql("DROP TABLE IF EXISTS source");
    validationCatalog.dropTable(tableIdent, true);
  }

  @Test
  public void testMergeConditionSplitIntoTargetPredicateAndJoinCondition() {
    testMergeConditionSplitIntoTargetPredicateAndJoinCondition(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testMergeWithStaticPredicatePushDown() {
    testMergeWithStaticPredicatePushDown(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testMergeIntoEmptyTargetInsertAllNonMatchingRows() {
    testMergeIntoEmptyTargetInsertAllNonMatchingRows(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testMergeIntoEmptyTargetInsertOnlyMatchingRows() {
    testMergeIntoEmptyTargetInsertOnlyMatchingRows(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testMergeWithOnlyUpdateClause() {
    testMergeWithOnlyUpdateClause(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testMergeWithOnlyUpdateClauseAndNullValues() {
    testMergeWithOnlyUpdateClauseAndNullValues(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testMergeWithOnlyDeleteClause() {
    testMergeWithOnlyDeleteClause(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testMergeWithAllCauses() {
    testMergeWithAllCauses(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testMergeWithAllCausesWithExplicitColumnSpecification() {
    testMergeWithAllCausesWithExplicitColumnSpecification(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testMergeWithSourceCTE() {
    testMergeWithSourceCTE(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testMergeWithSourceFromSetOps() {
    testMergeWithSourceFromSetOps(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testMergeWithMultipleUpdatesForTargetRowSmallTargetLargeSource() {
    testMergeWithMultipleUpdatesForTargetRowSmallTargetLargeSource(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void
      testMergeWithMultipleUpdatesForTargetRowSmallTargetLargeSourceEnabledHashShuffleJoin() {
    testMergeWithMultipleUpdatesForTargetRowSmallTargetLargeSourceEnabledHashShuffleJoin(
        SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testMergeWithMultipleUpdatesForTargetRowSmallTargetLargeSourceNoEqualityCondition() {
    testMergeWithMultipleUpdatesForTargetRowSmallTargetLargeSourceEnabledHashShuffleJoin(
        SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testMergeWithMultipleUpdatesForTargetRowSmallTargetLargeSourceNoNotMatchedActions() {
    testMergeWithMultipleUpdatesForTargetRowSmallTargetLargeSourceNoNotMatchedActions(
        SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void
      testMergeWithMultipleUpdatesForTargetRowSmallTargetLargeSourceNoNotMatchedActionsNoEqualityCondition() {
    testMergeWithMultipleUpdatesForTargetRowSmallTargetLargeSourceNoNotMatchedActionsNoEqualityCondition(
        SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testMergeWithMultipleUpdatesForTargetRow() {
    testMergeWithMultipleUpdatesForTargetRow(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testMergeWithUnconditionalDelete() {
    testMergeWithUnconditionalDelete(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testMergeWithSingleConditionalDelete() {
    testMergeWithSingleConditionalDelete(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testMergeWithIdentityTransform() {
    testMergeWithIdentityTransform(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testMergeWithDaysTransform() {
    testMergeWithDaysTransform(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testMergeWithBucketTransform() {
    testMergeWithBucketTransform(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testMergeWithTruncateTransform() {
    testMergeWithTruncateTransform(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testMergeIntoPartitionedAndOrderedTable() {
    testMergeIntoPartitionedAndOrderedTable(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testSelfMerge() {
    testSelfMerge(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testSelfMergeWithCaching() {
    testSelfMergeWithCaching(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testMergeWithSourceAsSelfSubquery() {
    testMergeWithSourceAsSelfSubquery(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public synchronized void testMergeWithSerializableIsolation() throws InterruptedException {
    testMergeWithSerializableIsolation(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public synchronized void testMergeWithSnapshotIsolation()
      throws InterruptedException, ExecutionException {
    testMergeWithSnapshotIsolation(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testMergeWithExtraColumnsInSource() {
    testMergeWithExtraColumnsInSource(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testMergeWithNullsInTargetAndSource() {
    testMergeWithNullsInTargetAndSource(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testMergeWithNullSafeEquals() {
    testMergeWithNullSafeEquals(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testMergeWithNullCondition() {
    testMergeWithNullCondition(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testMergeWithNullActionConditions() {
    testMergeWithNullActionConditions(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testMergeWithMultipleMatchingActions() {
    testMergeWithMultipleMatchingActions(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testMergeWithMultipleRowGroupsParquet() throws NoSuchTableException {
    testMergeWithMultipleRowGroupsParquet(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testMergeInsertOnly() {
    testMergeInsertOnly(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testMergeInsertOnlyWithCondition() {
    testMergeInsertOnlyWithCondition(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testMergeAlignsUpdateAndInsertActions() {
    testMergeAlignsUpdateAndInsertActions(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testMergeMixedCaseAlignsUpdateAndInsertActions() {
    testMergeMixedCaseAlignsUpdateAndInsertActions(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testMergeUpdatesNestedStructFields() {
    testMergeUpdatesNestedStructFields(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testMergeWithInferredCasts() {
    testMergeWithInferredCasts(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testMergeModifiesNullStruct() {
    testMergeModifiesNullStruct(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testMergeRefreshesRelationCache() {
    testMergeRefreshesRelationCache(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testMergeWithMultipleNotMatchedActions() {
    testMergeWithMultipleNotMatchedActions(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testMergeWithMultipleConditionalNotMatchedActions() {
    testMergeWithMultipleConditionalNotMatchedActions(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testMergeResolvesColumnsByName() {
    testMergeResolvesColumnsByName(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testMergeShouldResolveWhenThereAreNoUnresolvedExpressionsOrColumns() {
    testMergeShouldResolveWhenThereAreNoUnresolvedExpressionsOrColumns(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testMergeWithTableWithNonNullableColumn() {
    testMergeWithTableWithNonNullableColumn(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testMergeWithNonExistingColumns() {
    testMergeWithNonExistingColumns(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testMergeWithInvalidColumnsInInsert() {
    testMergeWithInvalidColumnsInInsert(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testMergeWithInvalidUpdates() {
    testMergeWithInvalidUpdates(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testMergeWithConflictingUpdates() {
    testMergeWithConflictingUpdates(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testMergeWithInvalidAssignments() {
    testMergeWithInvalidAssignments(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testMergeWithNonDeterministicConditions() {
    testMergeWithNonDeterministicConditions(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testMergeWithAggregateExpressions() {
    testMergeWithAggregateExpressions(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testMergeWithSubqueriesInConditions() {
    testMergeWithSubqueriesInConditions(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testMergeWithTargetColumnsInInsertConditions() {
    testMergeWithTargetColumnsInInsertConditions(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testMergeWithNonIcebergTargetTableNotSupported() {
    testMergeWithNonIcebergTargetTableNotSupported(SnapshotRef.MAIN_BRANCH);
  }

  /**
   * Tests a merge where both the source and target are evaluated to be partitioned by
   * SingePartition at planning time but DynamicFileFilterExec will return an empty target.
   */
  @Test
  public void testMergeSinglePartitionPartitioning() {
    testMergeSinglePartitionPartitioning(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testMergeEmptyTable() {
    testMergeEmptyTable(SnapshotRef.MAIN_BRANCH);
  }
}
