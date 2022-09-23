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

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.aws.dynamodb.DynamoDbLockManager;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.MoreExecutors;
import org.apache.iceberg.util.Tasks;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;

public class TestGlueCatalogLock extends GlueTestBase {

  private static String lockTableName;
  private static DynamoDbClient dynamo;

  @BeforeClass
  public static void beforeClass() {
    GlueTestBase.beforeClass();
    String testBucketPath = "s3://" + testBucketName + "/" + testPathPrefix;
    lockTableName = getRandomName();
    S3FileIO fileIO = new S3FileIO(clientFactory::s3);
    glueCatalog = new GlueCatalog();
    AwsProperties awsProperties = new AwsProperties();
    dynamo = clientFactory.dynamo();
    glueCatalog.initialize(
        catalogName,
        testBucketPath,
        awsProperties,
        glue,
        new DynamoDbLockManager(dynamo, lockTableName),
        fileIO,
        ImmutableMap.of());
  }

  @AfterClass
  public static void afterClass() {
    GlueTestBase.afterClass();
    dynamo.deleteTable(DeleteTableRequest.builder().tableName(lockTableName).build());
  }

  @Test
  public void testParallelCommitMultiThreadSingleCommit() {
    int nThreads = 20;
    String namespace = createNamespace();
    String tableName = getRandomName();
    createTable(namespace, tableName);
    Table table = glueCatalog.loadTable(TableIdentifier.of(namespace, tableName));
    DataFile dataFile =
        DataFiles.builder(partitionSpec)
            .withPath("/path/to/data-a.parquet")
            .withFileSizeInBytes(1)
            .withRecordCount(1)
            .build();

    List<AppendFiles> pendingCommits =
        IntStream.range(0, nThreads)
            .mapToObj(i -> table.newAppend().appendFile(dataFile))
            .collect(Collectors.toList());

    ExecutorService executorService =
        MoreExecutors.getExitingExecutorService(
            (ThreadPoolExecutor) Executors.newFixedThreadPool(nThreads));

    Tasks.range(nThreads)
        .retry(10000)
        .throwFailureWhenFinished()
        .executeWith(executorService)
        .run(i -> pendingCommits.get(i).commit());

    table.refresh();
    Assert.assertEquals(
        "Commits should all succeed sequentially", nThreads, table.history().size());
    Assert.assertEquals(
        "Should have all manifests",
        nThreads,
        table.currentSnapshot().allManifests(table.io()).size());
  }

  @Test
  public void testParallelCommitMultiThreadMultiCommit() {
    String namespace = createNamespace();
    String tableName = getRandomName();
    createTable(namespace, tableName);
    Table table = glueCatalog.loadTable(TableIdentifier.of(namespace, tableName));
    String fileName = UUID.randomUUID().toString();
    DataFile file =
        DataFiles.builder(table.spec())
            .withPath(FileFormat.PARQUET.addExtension(fileName))
            .withRecordCount(2)
            .withFileSizeInBytes(0)
            .build();

    ExecutorService executorService =
        MoreExecutors.getExitingExecutorService(
            (ThreadPoolExecutor) Executors.newFixedThreadPool(2));

    AtomicInteger barrier = new AtomicInteger(0);
    Tasks.range(2)
        .stopOnFailure()
        .throwFailureWhenFinished()
        .executeWith(executorService)
        .run(
            index -> {
              for (int numCommittedFiles = 0; numCommittedFiles < 10; numCommittedFiles++) {
                while (barrier.get() < numCommittedFiles * 2) {
                  try {
                    Thread.sleep(10);
                  } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                  }
                }

                table.newFastAppend().appendFile(file).commit();
                barrier.incrementAndGet();
              }
            });

    table.refresh();
    Assert.assertEquals("Commits should all succeed sequentially", 20, table.history().size());
    Assert.assertEquals(
        "should have 20 manifests", 20, table.currentSnapshot().allManifests(table.io()).size());
  }
}
