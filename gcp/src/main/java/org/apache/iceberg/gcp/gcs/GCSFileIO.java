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

import com.google.api.client.util.Lists;
import com.google.api.client.util.Maps;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.io.BulkDeletionFailureException;
import org.apache.iceberg.io.DelegateFileIO;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.StorageCredential;
import org.apache.iceberg.io.SupportsStorageCredentials;
import org.apache.iceberg.metrics.MetricsContext;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterators;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
import org.apache.iceberg.util.SerializableMap;
import org.apache.iceberg.util.SerializableSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FileIO Implementation backed by Google Cloud Storage (GCS)
 *
 * <p>Locations follow the conventions used by {@link
 * com.google.cloud.storage.BlobId#fromGsUtilUri(String) BlobId.fromGsUtilUri} that follow the
 * convention
 *
 * <pre>{@code gs://<bucket>/<blob_path>}</pre>
 *
 * <p>See <a href="https://cloud.google.com/storage/docs/folders#overview">Cloud Storage
 * Overview</a>
 */
public class GCSFileIO implements DelegateFileIO, SupportsStorageCredentials {
  private static final Logger LOG = LoggerFactory.getLogger(GCSFileIO.class);
  private static final String DEFAULT_METRICS_IMPL =
      "org.apache.iceberg.hadoop.HadoopMetricsContext";
  private static final String ROOT_STORAGE_PREFIX = "gs";

  private SerializableSupplier<Storage> storageSupplier;
  private MetricsContext metrics = MetricsContext.nullMetrics();
  private final AtomicBoolean isResourceClosed = new AtomicBoolean(false);
  private SerializableMap<String, String> properties = null;
  // use modifiable collection for Kryo serde
  private List<StorageCredential> storageCredentials = Lists.newArrayList();
  private transient volatile Map<String, PrefixedStorage> storageByPrefix;

  /**
   * No-arg constructor to load the FileIO dynamically.
   *
   * <p>All fields are initialized by calling {@link GCSFileIO#initialize(Map)} later.
   */
  public GCSFileIO() {}

  /**
   * Constructor with custom storage supplier.
   *
   * @param storageSupplier storage supplier
   */
  public GCSFileIO(SerializableSupplier<Storage> storageSupplier) {
    this.storageSupplier = storageSupplier;
    this.properties = SerializableMap.copyOf(Maps.newHashMap());
  }

  /**
   * Constructor with custom storage supplier and GCP properties.
   *
   * <p>Calling {@link GCSFileIO#initialize(Map)} will overwrite information set in this
   * constructor.
   *
   * @param storageSupplier storage supplier
   * @param gcpProperties gcp properties
   * @deprecated since 1.10.0, will be removed in 1.11.0; use {@link
   *     GCSFileIO#GCSFileIO(SerializableSupplier)} with {@link GCSFileIO#initialize(Map)} instead
   */
  @Deprecated
  public GCSFileIO(SerializableSupplier<Storage> storageSupplier, GCPProperties gcpProperties) {
    this.storageSupplier = storageSupplier;
    this.properties = SerializableMap.copyOf(gcpProperties.properties());
  }

  @Override
  public InputFile newInputFile(String path) {
    return GCSInputFile.fromLocation(path, clientForStoragePath(path), metrics);
  }

  @Override
  public InputFile newInputFile(String path, long length) {
    return GCSInputFile.fromLocation(path, length, clientForStoragePath(path), metrics);
  }

  @Override
  public OutputFile newOutputFile(String path) {
    return GCSOutputFile.fromLocation(path, clientForStoragePath(path), metrics);
  }

  @SuppressWarnings("resource")
  @Override
  public void deleteFile(String path) {
    // There is no specific contract about whether delete should fail
    // and other FileIO providers ignore failure.  Log the failure for
    // now as it is not a required operation for Iceberg.
    if (!clientForStoragePath(path).storage().delete(BlobId.fromGsUtilUri(path))) {
      LOG.warn("Failed to delete path: {}", path);
    }
  }

  @Override
  public Map<String, String> properties() {
    return properties.immutableMap();
  }

  public Storage client() {
    return client(ROOT_STORAGE_PREFIX);
  }

  @SuppressWarnings("resource")
  public Storage client(String storagePath) {
    return clientForStoragePath(storagePath).storage();
  }

  private PrefixedStorage clientForStoragePath(String storagePath) {
    PrefixedStorage client;
    String matchingPrefix = ROOT_STORAGE_PREFIX;

    for (String storagePrefix : storageByPrefix().keySet()) {
      if (storagePath.startsWith(storagePrefix)
          && storagePrefix.length() > matchingPrefix.length()) {
        matchingPrefix = storagePrefix;
      }
    }

    client = storageByPrefix().getOrDefault(matchingPrefix, null);

    Preconditions.checkState(
        null != client, "[BUG] GCS client for storage path not available: %s", storagePath);
    return client;
  }

  @Override
  public void initialize(Map<String, String> props) {
    this.properties = SerializableMap.copyOf(props);
    initMetrics(properties);
  }

  @SuppressWarnings("CatchBlockLogException")
  private void initMetrics(Map<String, String> props) {
    // Report Hadoop metrics if Hadoop is available
    try {
      DynConstructors.Ctor<MetricsContext> ctor =
          DynConstructors.builder(MetricsContext.class)
              .hiddenImpl(DEFAULT_METRICS_IMPL, String.class)
              .buildChecked();
      MetricsContext context = ctor.newInstance("gcs");
      context.initialize(props);
      this.metrics = context;
    } catch (NoClassDefFoundError | NoSuchMethodException | ClassCastException e) {
      LOG.warn(
          "Unable to load metrics class: '{}', falling back to null metrics", DEFAULT_METRICS_IMPL);
    }
  }

  private Map<String, PrefixedStorage> storageByPrefix() {
    if (null == storageByPrefix) {
      synchronized (this) {
        if (null == storageByPrefix) {
          Map<String, PrefixedStorage> localStorageByPrefix = Maps.newHashMap();

          localStorageByPrefix.put(
              ROOT_STORAGE_PREFIX,
              new PrefixedStorage(ROOT_STORAGE_PREFIX, properties, storageSupplier));
          storageCredentials.stream()
              .filter(c -> c.prefix().startsWith(ROOT_STORAGE_PREFIX))
              .collect(Collectors.toList())
              .forEach(
                  storageCredential -> {
                    Map<String, String> propertiesWithCredentials =
                        ImmutableMap.<String, String>builder()
                            .putAll(properties)
                            .putAll(storageCredential.config())
                            .buildKeepingLast();

                    localStorageByPrefix.put(
                        storageCredential.prefix(),
                        new PrefixedStorage(
                            storageCredential.prefix(),
                            propertiesWithCredentials,
                            storageSupplier));
                  });
          this.storageByPrefix = localStorageByPrefix;
        }
      }
    }

    return storageByPrefix;
  }

  @Override
  public void close() {
    // handles concurrent calls to close()
    if (isResourceClosed.compareAndSet(false, true)) {
      if (storageByPrefix != null) {
        storageByPrefix.values().forEach(PrefixedStorage::close);
        this.storageByPrefix = null;
      }
    }
  }

  @SuppressWarnings("resource")
  @Override
  public Iterable<FileInfo> listPrefix(String prefix) {
    GCSLocation location = new GCSLocation(prefix);
    return () ->
        clientForStoragePath(prefix)
            .storage()
            .list(location.bucket(), Storage.BlobListOption.prefix(location.prefix()))
            .streamAll()
            .map(
                blob ->
                    new FileInfo(
                        String.format("gs://%s/%s", blob.getBucket(), blob.getName()),
                        blob.getSize(),
                        createTimeMillis(blob)))
            .iterator();
  }

  private long createTimeMillis(Blob blob) {
    if (blob.getCreateTimeOffsetDateTime() == null) {
      return 0;
    }
    return blob.getCreateTimeOffsetDateTime().toInstant().toEpochMilli();
  }

  @Override
  public void deletePrefix(String prefix) {
    internalDeleteFiles(
        Streams.stream(listPrefix(prefix))
            .map(fileInfo -> BlobId.fromGsUtilUri(fileInfo.location())));
  }

  @Override
  public void deleteFiles(Iterable<String> pathsToDelete) throws BulkDeletionFailureException {
    internalDeleteFiles(Streams.stream(pathsToDelete).map(BlobId::fromGsUtilUri));
  }

  @SuppressWarnings("resource")
  private void internalDeleteFiles(Stream<BlobId> blobIdsToDelete) {
    Streams.stream(
            Iterators.partition(
                blobIdsToDelete.iterator(),
                clientForStoragePath(ROOT_STORAGE_PREFIX).gcpProperties().deleteBatchSize()))
        .forEach(
            batch -> {
              if (!batch.isEmpty()) {
                clientForStoragePath(batch.get(0).toGsUtilUri()).storage().delete(batch);
              }
            });
  }

  @Override
  public void setCredentials(List<StorageCredential> credentials) {
    Preconditions.checkArgument(credentials != null, "Invalid storage credentials: null");
    // copy credentials into a modifiable collection for Kryo serde
    this.storageCredentials = Lists.newArrayList(credentials);
  }

  @Override
  public List<StorageCredential> credentials() {
    return ImmutableList.copyOf(storageCredentials);
  }
}
