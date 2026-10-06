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
package org.apache.iceberg.vortex;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Configures Arrow's global memory-access properties for Vortex reads, mirroring the behavior of
 * {@code VectorizedSparkParquetReaders}. Must run before any Arrow buffer class is initialized
 * (both the Vortex-relocated and the engine-facing Arrow copies read these properties in static
 * initializers), so callers invoke {@link #ensureConfigured()} from their own static blocks.
 */
public final class VortexArrowProperties {

  private static final Logger LOG = LoggerFactory.getLogger(VortexArrowProperties.class);
  private static final String ENABLE_UNSAFE_MEMORY_ACCESS = "arrow.enable_unsafe_memory_access";
  private static final String ENABLE_UNSAFE_MEMORY_ACCESS_ENV = "ARROW_ENABLE_UNSAFE_MEMORY_ACCESS";
  private static final String ENABLE_NULL_CHECK_FOR_GET = "arrow.enable_null_check_for_get";
  private static final String ENABLE_NULL_CHECK_FOR_GET_ENV = "ARROW_ENABLE_NULL_CHECK_FOR_GET";

  private static volatile boolean configured = false;

  static {
    ensureConfigured();
  }

  private VortexArrowProperties() {}

  public static void ensureConfigured() {
    if (configured) {
      return;
    }

    synchronized (VortexArrowProperties.class) {
      if (configured) {
        return;
      }

      try {
        enableUnsafeMemoryAccess();
        disableNullCheckForGet();
      } catch (Exception e) {
        LOG.warn("Couldn't set Arrow properties, which may impact read performance", e);
      }

      configured = true;
    }
  }

  // enables unsafe memory access to avoid costly checks to see if index is within bounds
  // as long as it is not configured explicitly (see BoundsChecking in Arrow)
  private static void enableUnsafeMemoryAccess() {
    String value = confValue(ENABLE_UNSAFE_MEMORY_ACCESS, ENABLE_UNSAFE_MEMORY_ACCESS_ENV);
    if (value == null) {
      LOG.info("Enabling {}", ENABLE_UNSAFE_MEMORY_ACCESS);
      System.setProperty(ENABLE_UNSAFE_MEMORY_ACCESS, "true");
    } else {
      LOG.info("Unsafe memory access was configured explicitly: {}", value);
    }
  }

  // disables expensive null checks for every get call in favor of Iceberg nullability
  // as long as it is not configured explicitly (see NullCheckingForGet in Arrow)
  private static void disableNullCheckForGet() {
    String value = confValue(ENABLE_NULL_CHECK_FOR_GET, ENABLE_NULL_CHECK_FOR_GET_ENV);
    if (value == null) {
      LOG.info("Disabling {}", ENABLE_NULL_CHECK_FOR_GET);
      System.setProperty(ENABLE_NULL_CHECK_FOR_GET, "false");
    } else {
      LOG.info("Null checking for get calls was configured explicitly: {}", value);
    }
  }

  private static String confValue(String propName, String envName) {
    String propValue = System.getProperty(propName);
    if (propValue != null) {
      return propValue;
    }

    return System.getenv(envName);
  }
}
