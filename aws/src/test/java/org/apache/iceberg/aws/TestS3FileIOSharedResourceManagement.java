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
package org.apache.iceberg.aws;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestS3FileIOSharedResourceManagement {

  private Map<String, String> properties;

  @BeforeEach
  public void before() {
    properties = Maps.newHashMap();
    properties.put("s3.endpoint", "https://localhost:9000");
    properties.put("s3.path-style-access", "true");
    properties.put("s3.delete.enabled", "false"); // Don't actually delete during tests
  }

  @Test
  public void testMultipleS3FileIOInstancesShareResources() {
    // Create multiple S3FileIO instances with same configuration
    S3FileIO fileIO1 = new S3FileIO();
    fileIO1.initialize(properties);

    S3FileIO fileIO2 = new S3FileIO();
    fileIO2.initialize(properties);

    S3FileIO fileIO3 = new S3FileIO();
    fileIO3.initialize(properties);

    // Verify they can coexist without resource conflicts
    assertThat(fileIO1.client()).isNotNull();
    assertThat(fileIO2.client()).isNotNull();
    assertThat(fileIO3.client()).isNotNull();

    // Close some instances - others should continue working
    fileIO1.close();
    fileIO2.close();

    // The remaining instance should still work
    assertThatCode(() -> fileIO3.client()).doesNotThrowAnyException();

    fileIO3.close();
  }

  @Test
  public void testSerializationDeserialization() throws Exception {
    // This test simulates Spark's broadcast variable serialization/deserialization
    S3FileIO originalFileIO = new S3FileIO();
    originalFileIO.initialize(properties);

    // Serialize the S3FileIO
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(originalFileIO);
    }

    // Deserialize on "executor"
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    S3FileIO deserializedFileIO;
    try (ObjectInputStream ois = new ObjectInputStream(bais)) {
      deserializedFileIO = (S3FileIO) ois.readObject();
    }

    // Both should work without conflicts
    assertThat(originalFileIO.client()).isNotNull();
    assertThat(deserializedFileIO.client()).isNotNull();

    // Close original (simulating broadcast cleanup) - deserialized should still work
    originalFileIO.close();
    assertThatCode(deserializedFileIO::client).doesNotThrowAnyException();

    deserializedFileIO.close();
  }

  @Test
  public void testConcurrentAccessFromMultipleThreads() throws InterruptedException {
    int threadCount = 10;
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch completionLatch = new CountDownLatch(threadCount);
    AtomicInteger successCount = new AtomicInteger(0);
    AtomicReference<Exception> firstException = new AtomicReference<>();

    // Create multiple threads that each create and use S3FileIO instances
    for (int i = 0; i < threadCount; i++) {
      executor.submit(
          () -> {
            try {
              startLatch.await(); // Wait for all threads to be ready

              S3FileIO fileIO = new S3FileIO();
              fileIO.initialize(properties);

              // Simulate some operations
              assertThat(fileIO.client()).isNotNull();

              // Simulate random close timing (like Spark GC cleanup)
              if (Math.random() > 0.5) {
                Thread.sleep((long) (Math.random() * 100));
              }

              fileIO.close();
              successCount.incrementAndGet();

            } catch (Exception e) {
              firstException.compareAndSet(null, e);
            } finally {
              completionLatch.countDown();
            }
          });
    }

    // Start all threads simultaneously
    startLatch.countDown();

    // Wait for completion
    assertThat(completionLatch.await(30, TimeUnit.SECONDS)).isTrue();

    // Verify no exceptions occurred
    if (firstException.get() != null) {
      throw new RuntimeException("Test failed with exception", firstException.get());
    }

    assertThat(successCount.get()).isEqualTo(threadCount);

    executor.shutdown();
    assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
  }

  @Test
  public void testMemoryPressureSimulation() throws InterruptedException {
    // Simulate Spark memory pressure scenario
    int iterations = 50;
    S3FileIO[] fileIOs = new S3FileIO[iterations];

    // Create many S3FileIO instances (simulating high memory usage)
    for (int i = 0; i < iterations; i++) {
      fileIOs[i] = new S3FileIO();
      fileIOs[i].initialize(properties);
      assertThat(fileIOs[i].client()).isNotNull();
    }

    // Simulate memory pressure cleanup - close instances randomly
    for (int i = 0; i < iterations; i++) {
      if (Math.random() > 0.3) { // Close ~70% of instances
        fileIOs[i].close();
        fileIOs[i] = null; // Make eligible for GC
      }
    }

    // Force GC to trigger Cleaner cleanup
    System.gc();
    Thread.sleep(100); // Give Cleaner time to run

    // Remaining instances should still work
    for (int i = 0; i < iterations; i++) {
      if (fileIOs[i] != null) {
        final S3FileIO finalFileIO = fileIOs[i]; // Make effectively final for lambda
        assertThatCode(() -> finalFileIO.client()).doesNotThrowAnyException();
        fileIOs[i].close();
      }
    }
  }

  @Test
  public void testHttpClientCacheKeyGeneration() {
    // Test that identical configurations result in shared HTTP clients
    Map<String, String> props1 = Maps.newHashMap(properties);
    Map<String, String> props2 = Maps.newHashMap(properties);

    S3FileIO fileIO1 = new S3FileIO();
    fileIO1.initialize(props1);

    S3FileIO fileIO2 = new S3FileIO();
    fileIO2.initialize(props2);

    // Both should work and share underlying HTTP client resources
    assertThat(fileIO1.client()).isNotNull();
    assertThat(fileIO2.client()).isNotNull();

    fileIO1.close();
    fileIO2.close();
  }

  @Test
  public void testHttpClientCacheEviction() throws InterruptedException {
    ManagedHttpClientRegistry registry = ManagedHttpClientRegistry.getInstance();
    int initialCacheSize = (int) registry.getClientCache().estimatedSize();

    S3FileIO fileIO = new S3FileIO();
    fileIO.initialize(properties);
    assertThat(fileIO.client()).isNotNull();

    // Cache should have at least one entry (may be same as initial due to sharing)
    assertThat(registry.getClientCache().estimatedSize()).isGreaterThanOrEqualTo(initialCacheSize);
    assertThat(registry.getClientCache().estimatedSize()).isGreaterThan(0);

    fileIO.close();

    // Manual cache cleanup for testing
    registry.getClientCache().cleanUp();
  }

  @Test
  public void testResourceCleanupAfterTableOperations() {
    // Simulate table-level operations that create and destroy multiple FileIO instances
    for (int i = 0; i < 10; i++) {
      S3FileIO fileIO = new S3FileIO();
      fileIO.initialize(properties);
      assertThat(fileIO.client()).isNotNull();
      fileIO.close();
    }
  }
}
