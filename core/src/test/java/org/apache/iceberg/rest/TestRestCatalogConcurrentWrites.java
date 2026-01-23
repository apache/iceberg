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
package org.apache.iceberg.rest;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.gzip.GzipHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Test concurrent commit operations through the REST catalog to verify that sequence number
 * conflicts are properly handled with retryable exceptions.
 *
 * <p>This test creates multiple branches and performs parallel commits on each branch to verify
 * that the AssertLastSequenceNumber requirement properly catches sequence number conflicts and
 * throws CommitFailedException (retryable) instead of ValidationException (non-retryable).
 */
public class TestRestCatalogConcurrentWrites {
  private static final Namespace NS = Namespace.of("test");
  private static final TableIdentifier TABLE = TableIdentifier.of(NS, "test_table");

  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()), required(2, "data", Types.StringType.get()));

  private static final PartitionSpec SPEC =
      PartitionSpec.builderFor(SCHEMA).bucket("id", 16).build();

  @TempDir public Path temp;

  private RESTCatalog restCatalog;
  private InMemoryCatalog backendCatalog;
  private Server httpServer;

  @BeforeEach
  public void createCatalog() throws Exception {
    File warehouse = temp.toFile();

    this.backendCatalog = new InMemoryCatalog();
    this.backendCatalog.initialize(
        "in-memory",
        ImmutableMap.of(CatalogProperties.WAREHOUSE_LOCATION, warehouse.getAbsolutePath()));

    RESTCatalogAdapter adaptor = new RESTCatalogAdapter(backendCatalog);

    ServletContextHandler servletContext =
        new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
    servletContext.addServlet(new ServletHolder(new RESTCatalogServlet(adaptor)), "/*");
    servletContext.setHandler(new GzipHandler());

    this.httpServer = new Server(0);
    httpServer.setHandler(servletContext);
    httpServer.start();

    Configuration conf = new Configuration();
    SessionCatalog.SessionContext context =
        new SessionCatalog.SessionContext(
            UUID.randomUUID().toString(),
            "user",
            ImmutableMap.of("credential", "user:12345"),
            ImmutableMap.of());

    this.restCatalog =
        new RESTCatalog(
            context,
            (config) -> HTTPClient.builder(config).uri(config.get(CatalogProperties.URI)).build());
    restCatalog.setConf(conf);
    Map<String, String> properties =
        ImmutableMap.of(
            CatalogProperties.URI,
            httpServer.getURI().toString(),
            CatalogProperties.FILE_IO_IMPL,
            "org.apache.iceberg.inmemory.InMemoryFileIO",
            "credential",
            "catalog:12345");
    restCatalog.initialize("prod", properties);
  }

  @AfterEach
  public void closeCatalog() throws Exception {
    if (restCatalog != null) {
      restCatalog.close();
    }

    if (backendCatalog != null) {
      backendCatalog.close();
    }

    if (httpServer != null) {
      httpServer.stop();
      httpServer.join();
    }
  }

  /**
   * Test concurrent appends on multiple branches simultaneously to verify proper handling of
   * sequence number conflicts.
   *
   * <p>Creates 5 different branches on the table, then performs 10 parallel append commits on each
   * branch at the same time (50 total concurrent operations). This verifies that: 1. Sequence
   * number conflicts are caught by AssertLastSequenceNumber requirement 2. Conflicts result in
   * CommitFailedException (retryable) not ValidationException (non-retryable) 3. The REST catalog
   * properly handles concurrent modifications across different branches
   */
  @Test
  public void testConcurrentAppendsOnMultipleBranches() throws Exception {
    int numBranches = 5;
    int commitsPerBranch = 10;
    int totalConcurrentWrites = numBranches * commitsPerBranch;

    restCatalog.createNamespace(NS);
    Table table = restCatalog.buildTable(TABLE, SCHEMA).withPartitionSpec(SPEC).create();

    // Add initial data to the main branch
    DataFile initialFile =
        DataFiles.builder(SPEC)
            .withPath("/path/to/initial-data.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("id_bucket=0")
            .withRecordCount(2)
            .build();
    table.newFastAppend().appendFile(initialFile).commit();

    // Create 5 branches from the main branch
    String[] branchNames = new String[numBranches];
    for (int i = 0; i < numBranches; i++) {
      branchNames[i] = "branch-" + i;
      table.manageSnapshots().createBranch(branchNames[i]).commit();
    }

    // Refresh to get updated metadata with all branches
    table = restCatalog.loadTable(TABLE);

    ExecutorService executor = Executors.newFixedThreadPool(totalConcurrentWrites);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch doneLatch = new CountDownLatch(totalConcurrentWrites);

    AtomicInteger successCount = new AtomicInteger(0);
    AtomicInteger validationFailureCount = new AtomicInteger(0);
    AtomicInteger commitFailureCount = new AtomicInteger(0);
    AtomicInteger unexpectedFailureCount = new AtomicInteger(0);

    List<Future<?>> futures = Lists.newArrayList();

    try {
      // Launch concurrent appends - 10 per branch, all starting at the same time
      for (int branchIdx = 0; branchIdx < numBranches; branchIdx++) {
        final String branchName = branchNames[branchIdx];

        for (int commitIdx = 0; commitIdx < commitsPerBranch; commitIdx++) {
          final int finalBranchIdx = branchIdx;
          final int finalCommitIdx = commitIdx;

          Future<?> future =
              executor.submit(
                  () -> {
                    try {
                      // Wait for signal to start all threads simultaneously
                      startLatch.await();

                      // Each thread loads the table independently
                      Table localTable = restCatalog.loadTable(TABLE);

                      // Create a unique file for this commit
                      DataFile newFile =
                          DataFiles.builder(SPEC)
                              .withPath(
                                  String.format(
                                      "/path/to/branch-%d-commit-%d.parquet",
                                      finalBranchIdx, finalCommitIdx))
                              .withFileSizeInBytes(15)
                              .withPartitionPath(String.format("id_bucket=%d", finalBranchIdx % 16))
                              .withRecordCount(3)
                              .build();

                      // Append to the specific branch
                      localTable.newFastAppend().appendFile(newFile).toBranch(branchName).commit();

                      successCount.incrementAndGet();
                    } catch (BadRequestException e) {
                      // Sequence number validation errors wrapped in BadRequestException
                      // indicate the fix is not working properly
                      if (e.getMessage().contains("Cannot add snapshot with sequence number")) {
                        validationFailureCount.incrementAndGet();
                      } else {
                        unexpectedFailureCount.incrementAndGet();
                      }
                    } catch (ValidationException e) {
                      // ValidationException indicates the fix is not working properly
                      validationFailureCount.incrementAndGet();
                    } catch (CommitFailedException e) {
                      // CommitFailedException is expected - this is the correct behavior
                      // These will be automatically retried by the client
                      commitFailureCount.incrementAndGet();
                    } catch (Exception e) {
                      unexpectedFailureCount.incrementAndGet();
                    } finally {
                      doneLatch.countDown();
                    }
                  });
          futures.add(future);
        }
      }

      // Start all threads simultaneously
      startLatch.countDown();

      // Wait for all to complete
      boolean finished = doneLatch.await(180, TimeUnit.SECONDS);
      assertThat(finished).as("All processes should complete within timeout").isTrue();

      // Wait for all futures to complete
      for (Future<?> future : futures) {
        future.get(10, TimeUnit.SECONDS);
      }

      // Verify the fix: with AssertLastSequenceNumber, there should be NO validation failures
      // All concurrent conflicts should be caught as CommitFailedException (retryable)
      assertThat(validationFailureCount.get())
          .as(
              "With the fix, sequence number conflicts should be caught by AssertLastSequenceNumber "
                  + "and throw CommitFailedException (retryable), not ValidationException")
          .isEqualTo(0);

      // At least some should succeed (commits that don't conflict or succeed after retry)
      assertThat(successCount.get()).as("At least some appends should succeed").isGreaterThan(0);

      // CommitFailedExceptions are expected and will be retried by the client
      // We don't assert on the exact count since it depends on timing

      // No unexpected failures
      assertThat(unexpectedFailureCount.get())
          .as("Should have no unexpected failures")
          .isEqualTo(0);

    } finally {
      executor.shutdown();
      executor.awaitTermination(30, TimeUnit.SECONDS);
    }
  }
}
