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
package org.apache.iceberg.encryption;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import org.apache.iceberg.TableMetadata;

public interface EncryptionManagerFactory extends Closeable {

  /**
   * Initialize EncryptionManagerFactory from catalog properties.
   *
   * @param properties catalog properties
   */
  default void initialize(Map<String, String> properties) {}
  /**
   * Create encryption manager from table metadata.
   *
   * @param tableMetadata table metadata
   * @return created encryption manager instance.
   */
  EncryptionManager create(TableMetadata tableMetadata);

  /**
   * Close EncryptionManagerFactory to release underlying resources.
   *
   * <p>Calling this method is only required when this EncryptionManagerFactory instance is no
   * longer expected to be used, and the resources it holds need to be explicitly released to avoid
   * resource leaks.
   */
  @Override
  default void close() throws IOException {}
}
