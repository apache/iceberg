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
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.aws.s3.S3TestUtil;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
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

    AssertHelpers.assertThrows(
        "Commit failed exception should directly throw",
        CommitFailedException.class,
        "Datacenter on fire",
        () -> spyOps.commit(metadataV2, metadataV1));

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

    AssertHelpers.assertThrows(
        "Should throw CommitStateUnknownException since exception is unexpected",
        CommitStateUnknownException.class,
        "Datacenter on fire",
        () -> spyOps.commit(metadataV2, metadataV1));
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

    AssertHelpers.assertThrowsWithCause(
        "GlueCatalog should fail on concurrent modifications",
        CommitFailedException.class,
        "Glue detected concurrent update",
        ConcurrentModificationException.class,
        null,
        () -> spyOps.commit(metadataV2, metadataV1));
    Mockito.verify(spyOps, Mockito.times(0)).refresh();

    ops.refresh();
    Assert.assertEquals("Current metadata should not have changed", metadataV2, ops.current());
    Assert.assertTrue("Current metadata should still exist", metadataFileExists(metadataV2));
    Assert.assertEquals("No new metadata files should exist", 2, metadataFileCount(ops.current()));
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

    AssertHelpers.assertThrows(
        "Should throw CommitStateUnknownException since the catalog check was blocked",
        CommitStateUnknownException.class,
        "Datacenter on fire",
        () -> spyOps.commit(metadataV2, metadataV1));

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

    AssertHelpers.assertThrows(
        "Should throw CommitStateUnknownException since the catalog check was blocked",
        CommitStateUnknownException.class,
        "Datacenter on fire",
        () -> spyOps.commit(metadataV2, metadataV1));

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

  private void concurrentCommitAndThrowException(
      GlueTableOperations realOps, GlueTableOperations spyOperations, Table table) {
    // Simulate a communication error after a successful commit
    Mockito.doAnswer(
            i -> {
              Map<String, String> mapProperties = i.getArgument(1, Map.class);
              realOps.persistGlueTable(
                  i.getArgument(0, software.amazon.awssdk.services.glue.model.Table.class),
                  mapProperties,
                  i.getArgument(2, TableMetadata.class));

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
        .persistGlueTable(Mockito.any(), Mockito.anyMap(), Mockito.any());
  }

  @Test
  public void testCreateTableWithInvalidDB() {
    Table table = setupTable();
    GlueTableOperations ops = (GlueTableOperations) ((HasTableOperations) table).operations();

    TableMetadata metadataV1 = ops.current();
    TableMetadata metadataV2 = updateTable(table, ops);

    GlueTableOperations spyOps = Mockito.spy(ops);
    failCommitAndThrowException(spyOps, EntityNotFoundException.builder().build());

    AssertHelpers.assertThrows(
        "Should throw not found exception",
        NotFoundException.class,
        "because Glue cannot find the requested entity",
        () -> spyOps.commit(metadataV2, metadataV1));

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

    AssertHelpers.assertThrows(
        "Should throw forbidden exception",
        ForbiddenException.class,
        "because Glue cannot access the requested resources",
        () -> spyOps.commit(metadataV2, metadataV1));

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

    AssertHelpers.assertThrows(
        "Should throw validation exception",
        org.apache.iceberg.exceptions.ValidationException.class,
        "because Glue encountered a validation exception while accessing requested resources",
        () -> spyOps.commit(metadataV2, metadataV1));

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

    AssertHelpers.assertThrows(
        null, S3Exception.class, () -> spyOps.commit(metadataV2, metadataV1));

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

    AssertHelpers.assertThrows(
        null, GlueException.class, () -> spyOps.commit(metadataV2, metadataV1));

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

    AssertHelpers.assertThrows(
        null, CommitFailedException.class, () -> spyOps.commit(metadataV2, metadataV1));

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

  private void commitAndThrowException(GlueTableOperations realOps, GlueTableOperations spyOps) {
    Mockito.doAnswer(
            i -> {
              realOps.persistGlueTable(
                  i.getArgument(0, software.amazon.awssdk.services.glue.model.Table.class),
                  i.getArgument(1, Map.class),
                  i.getArgument(2, TableMetadata.class));
              throw new RuntimeException("Datacenter on fire");
            })
        .when(spyOps)
        .persistGlueTable(Mockito.any(), Mockito.anyMap(), Mockito.any());
  }

  private void failCommitAndThrowException(GlueTableOperations spyOps) {
    failCommitAndThrowException(spyOps, new RuntimeException("Datacenter on fire"));
  }

  private void failCommitAndThrowException(GlueTableOperations spyOps, Exception exceptionToThrow) {
    Mockito.doThrow(exceptionToThrow)
        .when(spyOps)
        .persistGlueTable(Mockito.any(), Mockito.anyMap(), Mockito.any());
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
