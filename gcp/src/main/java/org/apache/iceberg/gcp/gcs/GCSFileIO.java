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
import com.google.cloud.storage.StorageOptions;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.util.SerializableSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FileIO Implementation backed by Google Cloud Storage (GCS)
 * <p>
 * Locations follow the conventions used by
 * {@link com.google.cloud.storage.BlobId#fromGsUtilUri(String) BlobId.fromGsUtilUri}
 * that follow the convention <pre>{@code gs://<bucket>/<blob_path>}</pre>
 * <p>
 * See <a href="https://cloud.google.com/storage/docs/folders#overview">Cloud Storage Overview</a>
 */
public class GCSFileIO implements FileIO {
  private static final Logger LOG = LoggerFactory.getLogger(GCSFileIO.class);

  private SerializableSupplier<Storage> storageSupplier;
  private GCPProperties gcpProperties;
  private transient Storage storage;
  private final AtomicBoolean isResourceClosed = new AtomicBoolean(false);

  /**
   * No-arg constructor to load the FileIO dynamically.
   * <p>
   * All fields are initialized by calling {@link GCSFileIO#initialize(Map)} later.
   */
  public GCSFileIO() {
  }

  /**
   * Constructor with custom storage supplier and GCP properties.
   * <p>
   * Calling {@link GCSFileIO#initialize(Map)} will overwrite information set in this constructor.
   *
   * @param storageSupplier storage supplier
   * @param gcpProperties gcp properties
   */
  public GCSFileIO(SerializableSupplier<Storage> storageSupplier, GCPProperties gcpProperties) {
    this.storageSupplier = storageSupplier;
    this.gcpProperties = gcpProperties;
  }

  @Override
  public InputFile newInputFile(String path) {
    return GCSInputFile.fromLocation(path, client(), gcpProperties);
  }

  @Override
  public OutputFile newOutputFile(String path) {
    return GCSOutputFile.fromLocation(path, client(), gcpProperties);
  }

  @Override
  public void deleteFile(String path) {
    // There is no specific contract about whether delete should fail
    // and other FileIO providers ignore failure.  Log the failure for
    // now as it is not a required operation for Iceberg.
    if (!client().delete(BlobId.fromGsUtilUri(path))) {
      LOG.warn("Failed to delete path: {}", path);
    }
  }

  private Storage client() {
    if (storage == null) {
      storage = storageSupplier.get();
    }
    return storage;
  }

  @Override
  public void initialize(Map<String, String> properties) {
    this.gcpProperties = new GCPProperties(properties);

    this.storageSupplier = () -> {
      StorageOptions.Builder builder = StorageOptions.newBuilder();

      gcpProperties.projectId().ifPresent(builder::setProjectId);
      gcpProperties.clientLibToken().ifPresent(builder::setClientLibToken);
      gcpProperties.serviceHost().ifPresent(builder::setHost);

      return builder.build().getService();
    };
  }

  @Override
  public void close() {
    // handles concurrent calls to close()
    if (isResourceClosed.compareAndSet(false, true)) {
      if (storage != null) {
        // GCS Storage does not appear to be closable, so release the reference
        storage = null;
      }
    }
  }
}
