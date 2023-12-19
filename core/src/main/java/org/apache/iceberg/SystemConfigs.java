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
package org.apache.iceberg;

import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Configuration properties that are controlled by Java system properties or environmental variable.
 */
public class SystemConfigs {
  private static final Logger LOG = LoggerFactory.getLogger(SystemConfigs.class);

  private SystemConfigs() {}

  /**
   * Sets the size of the worker pool. The worker pool limits the number of tasks concurrently
   * processing manifests in the base table implementation across all concurrent planning or commit
   * operations.
   */
  public static final ConfigEntry<Integer> WORKER_THREAD_POOL_SIZE =
      new ConfigEntry<>(
          "iceberg.worker.num-threads",
          "ICEBERG_WORKER_NUM_THREADS",
          Math.max(2, Runtime.getRuntime().availableProcessors()),
          Integer::parseUnsignedInt);

  /**
   * Sets the size of the delete worker pool. This limits the number of threads used to compute the
   * PositionDeleteIndex from the position deletes for a data file.
   */
  public static final ConfigEntry<Integer> DELETE_WORKER_THREAD_POOL_SIZE =
      new ConfigEntry<>(
          "iceberg.worker.delete-num-threads",
          "ICEBERG_WORKER_DELETE_NUM_THREADS",
          Math.max(2, Runtime.getRuntime().availableProcessors()),
          Integer::parseUnsignedInt);

  /** Whether to use the shared worker pool when planning table scans. */
  public static final ConfigEntry<Boolean> SCAN_THREAD_POOL_ENABLED =
      new ConfigEntry<>(
          "iceberg.scan.plan-in-worker-pool",
          "ICEBERG_SCAN_PLAN_IN_WORKER_POOL",
          true,
          Boolean::parseBoolean);

  /**
   * Maximum number of distinct {@link org.apache.iceberg.io.FileIO} that is allowed to have
   * associated {@link org.apache.iceberg.io.ContentCache} in memory at a time.
   */
  public static final ConfigEntry<Integer> IO_MANIFEST_CACHE_MAX_FILEIO =
      new ConfigEntry<>(
          "iceberg.io.manifest.cache.fileio-max",
          "ICEBERG_IO_MANIFEST_CACHE_FILEIO_MAX",
          8,
          Integer::parseUnsignedInt);

  /** @deprecated will be removed in 2.0.0; use name mapping instead */
  @Deprecated
  public static final ConfigEntry<Boolean> NETFLIX_UNSAFE_PARQUET_ID_FALLBACK_ENABLED =
      new ConfigEntry<>(
          "iceberg.netflix.unsafe-parquet-id-fallback.enabled",
          "ICEBERG_NETFLIX_UNSAFE_PARQUET_ID_FALLBACK_ENABLED",
          true,
          s -> {
            LOG.warn(
                "Fallback ID assignment in Parquet is UNSAFE and will be removed in 2.0.0. Use name mapping instead.");
            return Boolean.parseBoolean(s);
          });

  public static class ConfigEntry<T> {
    private final String propertyKey;
    private final String envKey;
    private final T defaultValue;
    private final Function<String, T> parseFunc;
    private T lazyValue = null;

    private ConfigEntry(
        String propertyKey, String envKey, T defaultValue, Function<String, T> parseFunc) {
      this.propertyKey = propertyKey;
      this.envKey = envKey;
      this.defaultValue = defaultValue;
      this.parseFunc = parseFunc;
    }

    public final String propertyKey() {
      return propertyKey;
    }

    public final String envKey() {
      return envKey;
    }

    public final T defaultValue() {
      return defaultValue;
    }

    public final T value() {
      if (lazyValue == null) {
        lazyValue = produceValue();
      }

      return lazyValue;
    }

    private T produceValue() {
      String value = System.getProperty(propertyKey);
      if (value == null) {
        value = System.getenv(envKey);
      }

      if (value != null) {
        try {
          return parseFunc.apply(value);
        } catch (Exception e) {
          // will return the default value
          LOG.error(
              "Failed to parse the config value set by system property: {} or env variable: {}, "
                  + "using the default value: {}",
              propertyKey,
              envKey,
              defaultValue,
              e);
        }
      }

      return defaultValue;
    }
  }
}
