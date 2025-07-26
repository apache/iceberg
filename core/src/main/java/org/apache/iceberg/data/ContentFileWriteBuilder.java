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
package org.apache.iceberg.data;

import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptionKeyMetadata;
import org.apache.iceberg.io.DataWriter;

/**
 * A generic builder interface for creating specialized file writers in the Iceberg ecosystem.
 *
 * <p>This builder provides a unified configuration API for generating various types of content
 * writers:
 *
 * <ul>
 *   <li>{@link DataWriter} for creating data files with table records
 *   <li>{@link EqualityDeleteWriter} for creating files with equality-based delete records
 *   <li>{@link PositionDeleteWriter} for creating files with position-based delete records
 * </ul>
 *
 * <p>Each concrete implementation configures the underlying file format writer while adding
 * content-specific metadata and behaviors.
 *
 * @param <B> the concrete builder type for method chaining
 */
interface ContentFileWriteBuilder<B extends ContentFileWriteBuilder<B>> {

  /** Set the file schema. */
  B fileSchema(Schema fileSchema);

  /**
   * Set a writer configuration property which affects the writer behavior.
   *
   * @param property a writer config property name
   * @param value config value
   * @return this for method chaining
   */
  B set(String property, String value);

  /**
   * Adds the new properties to the writer configuration.
   *
   * @param properties a map of writer config properties
   * @return this for method chaining
   */
  B set(Map<String, String> properties);

  /**
   * Set a file metadata property in the created file.
   *
   * @param property a file metadata property name
   * @param value config value
   * @return this for method chaining
   */
  B meta(String property, String value);

  /**
   * Add the new properties to file metadata for the created file.
   *
   * @param properties a map of file metadata properties
   * @return this for method chaining
   */
  B meta(Map<String, String> properties);

  /** Sets the metrics configuration used for collecting column metrics for the created file. */
  B metricsConfig(MetricsConfig metricsConfig);

  /** Overwrite the file if it already exists. By default, overwrite is disabled. */
  B overwrite();

  /**
   * Sets the encryption key used for writing the file. If the writer does not support encryption,
   * then an exception should be thrown.
   */
  B fileEncryptionKey(ByteBuffer encryptionKey);

  /**
   * Sets the additional authentication data (AAD) prefix used for writing the file. If the writer
   * does not support encryption, then an exception should be thrown.
   */
  B fileAADPrefix(ByteBuffer aadPrefix);

  /** Sets the partition specification for the Iceberg metadata. */
  B spec(PartitionSpec newSpec);

  /** Sets the partition value for the Iceberg metadata. */
  B partition(StructLike partition);

  /** Sets the encryption key metadata for Iceberg metadata. */
  B keyMetadata(EncryptionKeyMetadata keyMetadata);

  /** Sets the sort order for the Iceberg metadata. */
  B sortOrder(SortOrder sortOrder);
}
