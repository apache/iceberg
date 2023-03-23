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
package org.apache.iceberg.io;

import java.io.Serializable;

/**
 * Interface for providing metadata file locations to read/write tasks.
 *
 * <p>Implementations must be {@link Serializable} because instances will be serialized to tasks.
 */
public interface MetadataLocationProvider extends Serializable {

  /**
   * Return a fully-qualified metadata location for a table.
   *
   * @return a fully-qualified data location URI
   */
  String metadataLocation();

  /**
   * Return a fully-qualified metadata file location for the given filename.
   *
   * @param filename a file name
   * @return a fully-qualified location URI for a metadata file
   */
  String newMetadataLocation(String filename);
}
