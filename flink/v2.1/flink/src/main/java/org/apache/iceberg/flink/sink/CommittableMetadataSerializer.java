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

import java.io.IOException;
import java.io.Serializable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

/**
 * Serializer for {@link CommittableMetadata} implementations.
 *
 * <p>Responsible for serializing and deserializing custom metadata that flows through the Iceberg
 * sink pipeline. The serializer must be registered via {@link CommittableMetadataRegistry} to be
 * used by the sink's serialization infrastructure.
 *
 * @see CommittableMetadata
 * @see CommittableMetadataRegistry
 */
public interface CommittableMetadataSerializer extends Serializable {
  /**
   * Serialize the given metadata to the output stream.
   *
   * @param metadata The metadata to serialize (never null)
   * @param out The output stream to write to
   * @throws IOException If serialization fails
   */
  void write(CommittableMetadata metadata, DataOutputView out) throws IOException;

  /**
   * Deserialize metadata from the input stream.
   *
   * @param in The input stream to read from
   * @return The deserialized metadata (never null)
   * @throws IOException If deserialization fails
   */
  CommittableMetadata read(DataInputView in) throws IOException;
}
