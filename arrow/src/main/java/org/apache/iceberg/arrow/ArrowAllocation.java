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
package org.apache.iceberg.arrow;

import org.apache.arrow.memory.RootAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ArrowAllocation {
  private static final Logger LOG = LoggerFactory.getLogger(ArrowAllocation.class);

  private static final String ALLOCATION_MANAGER_TYPE_PROPERTY =
      "arrow.memory.allocation.manager.type";

  static {
    // Set Arrow allocation manager to Netty if not already configured
    // This prevents Arrow's auto-detection from failing when classes are shaded
    // (e.g.; org.apache.iceberg.shaded.org.apache.arrow.*) since the path-based
    // detection in CheckAllocator.check() doesn't recognize shaded package structures
    String existingValue = System.getProperty(ALLOCATION_MANAGER_TYPE_PROPERTY);
    if (existingValue == null) {
      System.setProperty(ALLOCATION_MANAGER_TYPE_PROPERTY, "Netty");
      LOG.debug(
          "Setting system property {}=Netty for Arrow allocator compatibility with shaded dependencies",
          ALLOCATION_MANAGER_TYPE_PROPERTY);
    }

    ROOT_ALLOCATOR = createRootAllocator();
  }

  private static final RootAllocator ROOT_ALLOCATOR;

  private ArrowAllocation() {}

  private static RootAllocator createRootAllocator() {
    try {
      return new RootAllocator(Long.MAX_VALUE);
    } catch (Exception e) {
      throw new RuntimeException("Failed to initialize Arrow RootAllocator", e);
    }
  }

  public static RootAllocator rootAllocator() {
    return ROOT_ALLOCATOR;
  }
}
