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

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.metrics.MetricsContext;

class GCSInputFile extends BaseGCSFile implements InputFile {
  private Long length;

  static GCSInputFile fromLocation(
      String location, Storage storage, GCPProperties gcpProperties, MetricsContext metrics) {
    return new GCSInputFile(storage, BlobId.fromGsUtilUri(location), null, gcpProperties, metrics);
  }

  static GCSInputFile fromLocation(
      String location,
      long length,
      Storage storage,
      GCPProperties gcpProperties,
      MetricsContext metrics) {
    return new GCSInputFile(
        storage,
        BlobId.fromGsUtilUri(location),
        length > 0 ? length : null,
        gcpProperties,
        metrics);
  }

  GCSInputFile(
      Storage storage,
      BlobId blobId,
      Long length,
      GCPProperties gcpProperties,
      MetricsContext metrics) {
    super(storage, blobId, gcpProperties, metrics);
    this.length = length;
  }

  @Override
  public long getLength() {
    if (length == null) {
      this.length = getBlob().getSize();
    }

    return length;
  }

  @Override
  public SeekableInputStream newStream() {
    return new GCSInputStream(storage(), blobId(), gcpProperties(), metrics());
  }
}
