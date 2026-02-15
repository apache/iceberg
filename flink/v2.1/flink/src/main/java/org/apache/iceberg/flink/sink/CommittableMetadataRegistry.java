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
package org.apache.iceberg.flink.sink;

import javax.annotation.Nullable;

/**
 * Global registry for {@link CommittableMetadataSerializer} implementations.
 *
 * <p>This registry provides a location to register custom serializers for {@link
 * CommittableMetadata}. The registered serializer is used by {@link IcebergCommittableSerializer}
 * and {@link WriteResultSerializer} to serialize/deserialize metadata flowing through the pipeline.
 *
 * @see CommittableMetadata
 * @see CommittableMetadataSerializer
 */
public class CommittableMetadataRegistry {
  private static volatile CommittableMetadataSerializer serializer = null;

  private CommittableMetadataRegistry() {}

  /**
   * Register a metadata serializer.
   *
   * <p>This should be called before any Iceberg sinks are created.
   *
   * @param metadataSerializer The serializer to register (can be null to clear registration)
   */
  public static void register(@Nullable CommittableMetadataSerializer metadataSerializer) {
    serializer = metadataSerializer;
  }

  /**
   * Get the registered metadata serializer.
   *
   * @return The registered serializer, or null if none is registered
   */
  @Nullable
  public static CommittableMetadataSerializer get() {
    return serializer;
  }
}
