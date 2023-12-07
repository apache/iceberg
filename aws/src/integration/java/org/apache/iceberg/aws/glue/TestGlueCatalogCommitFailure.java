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
package org.apache.iceberg.aws.glue;

import java.io.File;
import java.util.Map;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.aws.s3.S3TestUtil;
import org.apache.iceberg.aws.util.RetryDetector;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.core.metrics.CoreMetric;
import software.amazon.awssdk.metrics.MetricCollector;
import software.amazon.awssdk.services.glue.model.AccessDeniedException;
import software.amazon.awssdk.services.glue.model.ConcurrentModificationException;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GlueException;
import software.amazon.awssdk.services.glue.model.ValidationException;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;

public class TestGlueCatalogCommitFailure extends GlueTestBase {

  @Test
  public void testFailedCommit() {
    Table table = setupTable();
    GlueTableOperations ops = (GlueTableOperations) ((HasTableOperations) table).operations();

    TableMetadata metadataV1 = ops.current();
    TableMetadata metadataV2 = updateTable(table, ops);

    GlueTableOperations spyOps = Mockito.spy(ops);
    failCommitAndThrowException(spyOps, new CommitFailedException("Datacenter on fire"));
    Assertions.assertThatThrownBy(() -> spyOps.commit(metadataV2, metadataV1))
        .isInstanceOf(CommitFailedException.class)
        .hasMessageContaining("Datacenter on fire");

    ops.refresh();
    Assert.assertEquals("Current metadata should not have changed", metadataV2, ops.current());
    Assert.assertTrue("Current metadata should still exist", metadataFileExists(metadataV2));
    Assert.assertEquals("No new metadata files should exist", 2, metadataFileCount(ops.current()));
  }

  @Test
  public void testFailedCommitThrowsUnknownException() {
    Table table = setupTable();
    GlueTableOperations ops = (GlueTableOperations) ((HasTableOperations) table).operations();

    TableMetadata metadataV1 = ops.current();
    TableMetadata metadataV2 = updateTable(table, ops);

    GlueTableOperations spyOps = Mockito.spy(ops);
    failCommitAndThrowException(spyOps);
    Assertions.assertThatThrownBy(() -> spyOps.commit(metadataV2, metadataV1))
        .isInstanceOf(CommitStateUnknownException.class)
        .hasMessageContaining("Datacenter on fire");
    Mockito.verify(spyOps, Mockito.times(1)).refresh();

    ops.refresh();
    Assert.assertEquals("Current metadata should not have changed", metadataV2, ops.current());
    Assert.assertTrue("Current metadata should still exist", metadataFileExists(metadataV2));
    Assert.assertEquals(
        "Client could not determine outcome so new metadata file should also exist",
        3,
        metadataFileCount(ops.current()));
  }

  @Test
  public void testConcurrentModificationExceptionDoesNotCheckCommitStatus() {
    Table table = setupTable();
    GlueTableOperations ops = (GlueTableOperations) ((HasTableOperations) table).operations();

    TableMetadata metadataV1 = ops.current();
    TableMetadata metadataV2 = updateTable(table, ops);

    GlueTableOperations spyOps = Mockito.spy(ops);
    failCommitAndThrowException(spyOps, ConcurrentModificationException.builder().build());
    Assertions.assertThatThrownBy(() -> spyOps.commit(metadataV2, metadataV1))
        .isInstanceOf(CommitFailedException.class)
        .hasMessageContaining("Glue detected concurrent update")
        .cause()
        .isInstanceOf(ConcurrentModificationException.class);
    Mockito.verify(spyOps, Mockito.times(0)).refresh();

    ops.refresh();
    Assert.assertEquals("Current metadata should not have changed", metadataV2, ops.current());
    Assert.assertTrue("Current metadata should still exist", metadataFileExists(metadataV2));
    Assert.assertEquals("No new metadata files should exist", 2, metadataFileCount(ops.current()));
  }

  @Test
  public void testCheckCommitStatusAfterRetries() {
    String namespace = createNamespace();
    String tableName = createTable(namespace);
    TableIdentifier tableId = TableIdentifier.of(namespace, tableName);

    GlueTableOperations spyOps =
        Mockito.spy((GlueTableOperations) glueCatalog.newTableOps(tableId));
    GlueCatalog spyCatalog = Mockito.spy(glueCatalog);
    Mockito.doReturn(spyOps).when(spyCatalog).newTableOps(Mockito.eq(tableId));
    Table table = spyCatalog.loadTable(tableId);

    TableMetadata metadataV1 = spyOps.current();
    simulateRetriedCommit(spyOps, true /* report retry */);
    updateTable(table, spyOps);

    Assert.assertNotEquals("Current metadata should have changed", metadataV1, spyOps.current());
    Assert.assertTrue("Current metadata should still exist", metadataFileExists(spyOps.current()));
    Assert.assertEquals(
        "No new metadata files should exist", 2, metadataFileCount(spyOps.current()));
  }

  @Test
  public void testNoRetryAwarenessCorruptsTable() {
    // This test exists to replicate the issue the prior test validates the fix for
    // See https://github.com/apache/iceberg/issues/7151
    String namespace = createNamespace();
    String tableName = createTable(namespace);
    TableIdentifier tableId = TableIdentifier.of(namespace, tableName);

    GlueTableOperations spyOps =
        Mockito.spy((GlueTableOperations) glueCatalog.newTableOps(tableId));
    GlueCatalog spyCatalog = Mockito.spy(glueCatalog);
    Mockito.doReturn(spyOps).when(spyCatalog).newTableOps(Mockito.eq(tableId));
    Table table = spyCatalog.loadTable(tableId);

    // Its possible that Glue or DynamoDB might someday make changes that render the retry detection
    // mechanism unnecessary. At that time, this test would start failing while the prior one would
    // still work. If or when that happens, we can re-evaluate whether the mechanism is still
    // necessary.
    simulateRetriedCommit(spyOps, false /* hide retry */);
    Assertions.assertThatThrownBy(() -> updateTable(table, spyOps))
        .as("Hidden retry causes writer to conflict with itself")
        .isInstanceOf(CommitFailedException.class)
        .hasMessageContaining("Glue detected concurrent update")
        .cause()
        .isInstanceOf(ConcurrentModificationException.class);

    Assertions.assertThatThrownBy(() -> glueCatalog.loadTable(tableId))
        .as("Table still accessible despite hidden retry, underlying assumptions may have changed")
        .isInstanceOf(NotFoundException.class)
        .hasMessageContaining("Location does not exist");
  }

  private void simulateRetriedCommit(GlueTableOperations spyOps, boolean reportRetry) {
    // Perform a successful commit, then call it again, optionally letting the retryDetector know
    // about it
    Mockito.doAnswer(
            i -> {
              final MetricCollector metrics = MetricCollector.create("test");
              metrics.reportMetric(CoreMetric.RETRY_COUNT, reportRetry ? 1 : 0);

              i.callRealMethod();
              i.getArgument(3, RetryDetector.class).publish(metrics.collect());
              i.callRealMethod();
              return null;
            })
        .when(spyOps)
        .persistGlueTable(Mockito.any(), Mockito.anyMap(), Mockito.any(), Mockito.any());
  }

  @Test
  public void testCommitThrowsExceptionWhileSucceeded() {
    Table table = setupTable();
    GlueTableOperations ops = (GlueTableOperations) ((HasTableOperations) table).operations();

    TableMetadata metadataV1 = ops.current();
    TableMetadata metadataV2 = updateTable(table, ops);

    GlueTableOperations spyOps = Mockito.spy(ops);

    // Simulate a communication error after a successful commit
    commitAndThrowException(ops, spyOps);

    // Shouldn't throw because the commit actually succeeds even though persistTable throws an
    // exception
    spyOps.commit(metadataV2, metadataV1);

    ops.refresh();
    Assert.assertNotEquals("Current metadata should have changed", metadataV2, ops.current());
    Assert.assertTrue(
        "Current metadata file should still exist", metadataFileExists(ops.current()));
    Assert.assertEquals(
        "Commit should have been successful and new metadata file should be made",
        3,
        metadataFileCount(ops.current()));
  }

  @Test
  public void testFailedCommitThrowsUnknownExceptionWhenStatusCheckFails() {
    Table table = setupTable();
    GlueTableOperations ops = (GlueTableOperations) ((HasTableOperations) table).operations();

    TableMetadata metadataV1 = ops.current();
    TableMetadata metadataV2 = updateTable(table, ops);

    GlueTableOperations spyOps = Mockito.spy(ops);
    failCommitAndThrowException(spyOps);
    breakFallbackCatalogCommitCheck(spyOps);
    Assertions.assertThatThrownBy(() -> spyOps.commit(metadataV2, metadataV1))
        .isInstanceOf(CommitStateUnknownException.class)
        .hasMessageContaining("Datacenter on fire");

    ops.refresh();

    Assert.assertEquals("Current metadata should not have changed", metadataV2, ops.current());
    Assert.assertTrue(
        "Current metadata file should still exist", metadataFileExists(ops.current()));
    Assert.assertEquals(
        "Client could not determine outcome so new metadata file should also exist",
        3,
        metadataFileCount(ops.current()));
  }

  @Test
  public void testSucceededCommitThrowsUnknownException() {
    Table table = setupTable();
    GlueTableOperations ops = (GlueTableOperations) ((HasTableOperations) table).operations();

    TableMetadata metadataV1 = ops.current();
    TableMetadata metadataV2 = updateTable(table, ops);

    GlueTableOperations spyOps = Mockito.spy(ops);
    commitAndThrowException(ops, spyOps);
    breakFallbackCatalogCommitCheck(spyOps);
    Assertions.assertThatThrownBy(() -> spyOps.commit(metadataV2, metadataV1))
        .isInstanceOf(CommitStateUnknownException.class)
        .hasMessageContaining("Datacenter on fire");
    ops.refresh();

    Assert.assertNotEquals("Current metadata should have changed", ops.current(), metadataV2);
    Assert.assertTrue(
        "Current metadata file should still exist", metadataFileExists(ops.current()));
  }

  /**
   * Pretends we threw an exception while persisting, the commit succeeded, the lock expired, and a
   * second committer placed a commit on top of ours before the first committer was able to check if
   * their commit succeeded or not
   *
   * <p>Timeline: Client 1 commits which throws an exception but suceeded Client 1's lock expires
   * while waiting to do the recheck for commit success Client 2 acquires a lock, commits
   * successfully on top of client 1's commit and release lock Client 1 check's to see if their
   * commit was successful
   *
   * <p>This tests to make sure a disconnected client 1 doesn't think their commit failed just
   * because it isn't the current one during the recheck phase.
   */
  @Test
  public void testExceptionThrownInConcurrentCommit() {
    Table table = setupTable();
    GlueTableOperations ops = (GlueTableOperations) ((HasTableOperations) table).operations();

    TableMetadata metadataV1 = ops.current();
    TableMetadata metadataV2 = updateTable(table, ops);

    GlueTableOperations spyOps = Mockito.spy(ops);
    concurrentCommitAndThrowException(ops, spyOps, table);

    /*
    This commit and our concurrent commit should succeed even though this commit throws an exception
    after the persist operation succeeds
     */
    spyOps.commit(metadataV2, metadataV1);

    ops.refresh();
    Assert.assertNotEquals("Current metadata should have changed", metadataV2, ops.current());
    Assert.assertTrue(
        "Current metadata file should still exist", metadataFileExists(ops.current()));
    Assert.assertEquals(
        "The column addition from the concurrent commit should have been successful",
        2,
        ops.current().schema().columns().size());
  }

  @SuppressWarnings("unchecked")
  private void concurrentCommitAndThrowException(
      GlueTableOperations realOps, GlueTableOperations spyOperations, Table table) {
    // Simulate a communication error after a successful commit
    Mockito.doAnswer(
            i -> {
              Map<String, String> mapProperties = i.getArgument(1, Map.class);
              realOps.persistGlueTable(
                  i.getArgument(0, software.amazon.awssdk.services.glue.model.Table.class),
                  mapProperties,
                  i.getArgument(2, TableMetadata.class),
                  i.getArgument(3, RetryDetector.class));

              // new metadata location is stored in map property, and used for locking
              String newMetadataLocation =
                  mapProperties.get(BaseMetastoreTableOperations.METADATA_LOCATION_PROP);

              // Simulate lock expiration or removal, use commit status null to avoid deleting data
              realOps.cleanupMetadataAndUnlock(null, newMetadataLocation);

              table.refresh();
              table.updateSchema().addColumn("newCol", Types.IntegerType.get()).commit();
              throw new RuntimeException("Datacenter on fire");
            })
        .when(spyOperations)
        .persistGlueTable(Mockito.any(), Mockito.anyMap(), Mockito.any(), Mockito.any());
  }

  @Test
  public void testCreateTableWithInvalidDB() {
    Table table = setupTable();
    GlueTableOperations ops = (GlueTableOperations) ((HasTableOperations) table).operations();

    TableMetadata metadataV1 = ops.current();
    TableMetadata metadataV2 = updateTable(table, ops);

    GlueTableOperations spyOps = Mockito.spy(ops);
    failCommitAndThrowException(spyOps, EntityNotFoundException.builder().build());
    Assertions.assertThatThrownBy(() -> spyOps.commit(metadataV2, metadataV1))
        .isInstanceOf(NotFoundException.class)
        .hasMessageContaining("because Glue cannot find the requested entity");

    ops.refresh();
    Assert.assertEquals("Current metadata should not have changed", metadataV2, ops.current());
    Assert.assertTrue("Current metadata should still exist", metadataFileExists(metadataV2));
    Assert.assertEquals("No new metadata files should exist", 2, metadataFileCount(ops.current()));
  }

  @Test
  public void testGlueAccessDeniedException() {
    Table table = setupTable();
    GlueTableOperations ops = (GlueTableOperations) ((HasTableOperations) table).operations();

    TableMetadata metadataV1 = ops.current();
    TableMetadata metadataV2 = updateTable(table, ops);

    GlueTableOperations spyOps = Mockito.spy(ops);
    failCommitAndThrowException(spyOps, AccessDeniedException.builder().build());
    Assertions.assertThatThrownBy(() -> spyOps.commit(metadataV2, metadataV1))
        .isInstanceOf(ForbiddenException.class)
        .hasMessageContaining("because Glue cannot access the requested resources");
    ops.refresh();
    Assert.assertEquals("Current metadata should not have changed", metadataV2, ops.current());
    Assert.assertTrue("Current metadata should still exist", metadataFileExists(metadataV2));
    Assert.assertEquals("No new metadata files should exist", 2, metadataFileCount(ops.current()));
  }

  @Test
  public void testGlueValidationException() {
    Table table = setupTable();
    GlueTableOperations ops = (GlueTableOperations) ((HasTableOperations) table).operations();

    TableMetadata metadataV1 = ops.current();
    TableMetadata metadataV2 = updateTable(table, ops);

    GlueTableOperations spyOps = Mockito.spy(ops);
    failCommitAndThrowException(spyOps, ValidationException.builder().build());
    Assertions.assertThatThrownBy(() -> spyOps.commit(metadataV2, metadataV1))
        .isInstanceOf(org.apache.iceberg.exceptions.ValidationException.class)
        .hasMessageContaining(
            "because Glue encountered a validation exception while accessing requested resources");

    ops.refresh();
    Assert.assertEquals("Current metadata should not have changed", metadataV2, ops.current());
    Assert.assertTrue("Current metadata should still exist", metadataFileExists(metadataV2));
    Assert.assertEquals("No new metadata files should exist", 2, metadataFileCount(ops.current()));
  }

  @Test
  public void testS3Exception() {
    Table table = setupTable();
    GlueTableOperations ops = (GlueTableOperations) ((HasTableOperations) table).operations();

    TableMetadata metadataV1 = ops.current();
    TableMetadata metadataV2 = updateTable(table, ops);

    GlueTableOperations spyOps = Mockito.spy(ops);
    failCommitAndThrowException(spyOps, S3Exception.builder().statusCode(300).build());
    Assertions.assertThatThrownBy(() -> spyOps.commit(metadataV2, metadataV1))
        .isInstanceOf(S3Exception.class)
        .hasMessage(null);
    ops.refresh();
    Assert.assertEquals("Current metadata should not have changed", metadataV2, ops.current());
    Assert.assertTrue("Current metadata should still exist", metadataFileExists(metadataV2));
    Assert.assertEquals("No new metadata files should exist", 2, metadataFileCount(ops.current()));
  }

  @Test
  public void testOtherGlueException() {
    Table table = setupTable();
    GlueTableOperations ops = (GlueTableOperations) ((HasTableOperations) table).operations();

    TableMetadata metadataV1 = ops.current();
    TableMetadata metadataV2 = updateTable(table, ops);

    GlueTableOperations spyOps = Mockito.spy(ops);
    failCommitAndThrowException(spyOps, GlueException.builder().statusCode(300).build());
    Assertions.assertThatThrownBy(() -> spyOps.commit(metadataV2, metadataV1))
        .isInstanceOf(GlueException.class)
        .hasMessage(null);

    ops.refresh();
    Assert.assertEquals("Current metadata should not have changed", metadataV2, ops.current());
    Assert.assertTrue("Current metadata should still exist", metadataFileExists(metadataV2));
    Assert.assertEquals("No new metadata files should exist", 2, metadataFileCount(ops.current()));
  }

  @Test
  public void testInternalServerErrorRetryCommit() {
    Table table = setupTable();
    GlueTableOperations ops = (GlueTableOperations) ((HasTableOperations) table).operations();

    TableMetadata metadataV1 = ops.current();
    TableMetadata metadataV2 = updateTable(table, ops);

    GlueTableOperations spyOps = Mockito.spy(ops);
    failCommitAndThrowException(spyOps, GlueException.builder().statusCode(500).build());
    Assertions.assertThatThrownBy(() -> spyOps.commit(metadataV2, metadataV1))
        .isInstanceOf(CommitFailedException.class)
        .hasMessage(null);

    ops.refresh();
    Assert.assertEquals("Current metadata should not have changed", metadataV2, ops.current());
    Assert.assertTrue("Current metadata should still exist", metadataFileExists(metadataV2));
    Assert.assertEquals("No new metadata files should exist", 2, metadataFileCount(ops.current()));
  }

  private Table setupTable() {
    String namespace = createNamespace();
    String tableName = createTable(namespace);
    return glueCatalog.loadTable(TableIdentifier.of(namespace, tableName));
  }

  private TableMetadata updateTable(Table table, GlueTableOperations ops) {
    table.updateSchema().addColumn("n", Types.IntegerType.get()).commit();

    ops.refresh();

    TableMetadata metadataV2 = ops.current();

    Assert.assertEquals(2, metadataV2.schema().columns().size());
    return metadataV2;
  }

  @SuppressWarnings("unchecked")
  private void commitAndThrowException(GlueTableOperations realOps, GlueTableOperations spyOps) {
    Mockito.doAnswer(
            i -> {
              realOps.persistGlueTable(
                  i.getArgument(0, software.amazon.awssdk.services.glue.model.Table.class),
                  i.getArgument(1, Map.class),
                  i.getArgument(2, TableMetadata.class),
                  i.getArgument(3, RetryDetector.class));
              throw new RuntimeException("Datacenter on fire");
            })
        .when(spyOps)
        .persistGlueTable(Mockito.any(), Mockito.anyMap(), Mockito.any(), Mockito.any());
  }

  private void failCommitAndThrowException(GlueTableOperations spyOps) {
    failCommitAndThrowException(spyOps, new RuntimeException("Datacenter on fire"));
  }

  private void failCommitAndThrowException(GlueTableOperations spyOps, Exception exceptionToThrow) {
    Mockito.doThrow(exceptionToThrow)
        .when(spyOps)
        .persistGlueTable(Mockito.any(), Mockito.anyMap(), Mockito.any(), Mockito.any());
  }

  private void breakFallbackCatalogCommitCheck(GlueTableOperations spyOperations) {
    Mockito.when(spyOperations.refresh())
        .thenThrow(new RuntimeException("Still on fire")); // Failure on commit check
  }

  private boolean metadataFileExists(TableMetadata metadata) {
    try {
      s3.headObject(
          HeadObjectRequest.builder()
              .bucket(S3TestUtil.getBucketFromUri(metadata.metadataFileLocation()))
              .key(S3TestUtil.getKeyFromUri(metadata.metadataFileLocation()))
              .build());
      return true;
    } catch (NoSuchKeyException e) {
      return false;
    }
  }

  private int metadataFileCount(TableMetadata metadata) {
    return (int)
        s3
            .listObjectsV2(
                ListObjectsV2Request.builder()
                    .bucket(S3TestUtil.getBucketFromUri(metadata.metadataFileLocation()))
                    .prefix(
                        new File(S3TestUtil.getKeyFromUri(metadata.metadataFileLocation()))
                            .getParent())
                    .build())
            .contents().stream()
            .filter(s3Object -> s3Object.key().endsWith("metadata.json"))
            .count();
  }
}
