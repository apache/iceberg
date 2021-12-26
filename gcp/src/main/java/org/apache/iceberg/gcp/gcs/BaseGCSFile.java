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

package org.apache.iceberg.gcp.gcs;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import java.net.URI;
import org.apache.iceberg.gcp.GCPProperties;

abstract class BaseGCSFile {
  private final Storage storage;
  private final GCPProperties gcpProperties;
  private final BlobId blobId;
  private Blob metadata;

  BaseGCSFile(Storage storage, BlobId blobId, GCPProperties gcpProperties) {
    this.storage = storage;
    this.blobId = blobId;
    this.gcpProperties = gcpProperties;
  }

  public String location() {
    return blobId.toGsUtilUri();
  }

  Storage storage() {
    return storage;
  }

  URI uri() {
    return URI.create(blobId.toGsUtilUri());
  }

  BlobId blobId() {
    return blobId;
  }

  protected GCPProperties gcpProperties() {
    return gcpProperties;
  }

  public boolean exists() {
    return getBlob() != null;
  }

  protected Blob getBlob() {
    if (metadata == null) {
      metadata = storage.get(blobId);
    }

    return metadata;
  }

  @Override
  public String toString() {
    return blobId.toString();
  }
}
