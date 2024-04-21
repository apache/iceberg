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

import java.io.Serializable;

public class MetadataFile implements Serializable {
  private final String location;
  private final String wrappedKeyMetadata;
  private final long size;

  public MetadataFile(String location, String wrappedKeyMetadata, long size) {
    this.location = location;
    this.wrappedKeyMetadata = wrappedKeyMetadata;
    this.size = size;
  }

  /** Location of metadata file. */
  public String location() {
    return location;
  }

  /**
   * Key metadata for metadata file in encrypted table. If contains the file encryption key - this
   * key cannot be open, must be wrapped (encrypted) with the table master key.
   *
   * @return base64-encoded key metadata for the metadata file
   */
  public String wrappedKeyMetadata() {
    return wrappedKeyMetadata;
  }

  /**
   * In encrypted tables, return the size of metadata file. Must be a verified value, taken from a
   * trusted source. In unencrypted tables, can return 0.
   */
  public long size() {
    return size;
  }
}
