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

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iceberg.SystemConfigs;
import org.junit.jupiter.api.Test;

/**
 * Tests for the {@link CatalogHandlers#ASYNC_PLANNING_THREADS} configuration and related system
 * properties.
 */
public class TestAsyncPlanningPoolConfig {

  @Test
  public void testDefaultAsyncPlanningThreadsIsReasonable() {
    // The default should be at least 2 to enable parallelism
    int defaultThreads = SystemConfigs.REST_ASYNC_PLANNING_THREADS.defaultValue();
    assertThat(defaultThreads)
        .as("Default async planning threads should be at least 2")
        .isGreaterThanOrEqualTo(2);
  }

  @Test
  public void testDefaultMatchesAvailableProcessors() {
    int expected = Math.max(2, Runtime.getRuntime().availableProcessors());
    int defaultThreads = SystemConfigs.REST_ASYNC_PLANNING_THREADS.defaultValue();
    assertThat(defaultThreads)
        .as("Default async planning threads should be max(2, availableProcessors) = %d", expected)
        .isEqualTo(expected);
  }

  @Test
  public void testPropertyKeyIsCorrect() {
    String propertyKey = SystemConfigs.REST_ASYNC_PLANNING_THREADS.propertyKey();
    assertThat(propertyKey)
        .as("System property key should follow Iceberg convention")
        .isEqualTo("iceberg.rest.async-planning-threads");
  }

  @Test
  public void testEnvVarKeyIsCorrect() {
    String envKey = SystemConfigs.REST_ASYNC_PLANNING_THREADS.envKey();
    assertThat(envKey)
        .as("Environment variable key should follow Iceberg convention")
        .isEqualTo("ICEBERG_REST_ASYNC_PLANNING_THREADS");
  }

  @Test
  public void testCatalogHandlersExposesThreadCount() {
    // Verify the constant is publicly accessible for server implementations
    assertThat(CatalogHandlers.ASYNC_PLANNING_THREADS)
        .as("CatalogHandlers should expose the configured thread count")
        .isGreaterThanOrEqualTo(2);
  }
}
