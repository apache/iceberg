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
package org.apache.iceberg.aws.s3;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.iceberg.aws.S3FileIOAwsClientFactories;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.io.BulkDeletionFailureException;
import org.apache.iceberg.io.CredentialSupplier;
import org.apache.iceberg.io.DelegateFileIO;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.StorageCredential;
import org.apache.iceberg.io.SupportsRecoveryOperations;
import org.apache.iceberg.io.SupportsStorageCredentials;
import org.apache.iceberg.metrics.MetricsContext;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Multimaps;
import org.apache.iceberg.relocated.com.google.common.collect.SetMultimap;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.SerializableMap;
import org.apache.iceberg.util.SerializableSupplier;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.GetObjectTaggingRequest;
import software.amazon.awssdk.services.s3.model.GetObjectTaggingResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.ObjectVersion;
import software.amazon.awssdk.services.s3.model.PutObjectTaggingRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.Tag;
import software.amazon.awssdk.services.s3.model.Tagging;
import software.amazon.awssdk.services.s3.paginators.ListObjectVersionsIterable;

/**
 * FileIO implementation backed by S3.
 *
 * <p>Locations used must follow the conventions for S3 URIs (e.g. s3://bucket/path...). URIs with
 * schemes s3a, s3n, https are also treated as s3 file paths. Using this FileIO with other schemes
 * will result in {@link org.apache.iceberg.exceptions.ValidationException}.
 */
public class S3FileIO
    implements CredentialSupplier,
        DelegateFileIO,
        SupportsRecoveryOperations,
        SupportsStorageCredentials {
  private static final Logger LOG = LoggerFactory.getLogger(S3FileIO.class);
  private static final String DEFAULT_METRICS_IMPL =
      "org.apache.iceberg.hadoop.HadoopMetricsContext";
  private static final String ROOT_PREFIX = "s3";
  private static volatile ExecutorService executorService;

  private String credential = null;
  private SerializableSupplier<S3Client> s3;
  private SerializableSupplier<S3AsyncClient> s3Async;
  private SerializableMap<String, String> properties = null;
  private MetricsContext metrics = MetricsContext.nullMetrics();
  private final AtomicBoolean isResourceClosed = new AtomicBoolean(false);
  private transient StackTraceElement[] createStack;
  // use modifiable collection for Kryo serde
  private List<StorageCredential> storageCredentials = Lists.newArrayList();
  private transient volatile Map<String, PrefixedS3Client> clientByPrefix;

  /**
   * No-arg constructor to load the FileIO dynamically.
   *
   * <p>All fields are initialized by calling {@link S3FileIO#initialize(Map)} later.
   */
  public S3FileIO() {}

  /**
   * Constructor with custom s3 supplier and S3FileIO properties.
   *
   * <p>Calling {@link S3FileIO#initialize(Map)} will overwrite information set in this constructor.
   *
   * @param s3 s3 supplier
   */
  public S3FileIO(SerializableSupplier<S3Client> s3) {
    this(s3, null, new S3FileIOProperties());
  }

  /**
   * Constructor with custom s3 supplier and s3Async supplier.
   *
   * <p>Calling {@link S3FileIO#initialize(Map)} will overwrite information set in this constructor.
   *
   * @param s3 s3 supplier
   * @param s3Async s3Async supplier
   */
  public S3FileIO(SerializableSupplier<S3Client> s3, SerializableSupplier<S3AsyncClient> s3Async) {
    this(s3, s3Async, new S3FileIOProperties());
  }

  /**
   * Constructor with custom s3 supplier and S3FileIO properties.
   *
   * <p>Calling {@link S3FileIO#initialize(Map)} will overwrite information set in this constructor.
   *
   * @param s3 s3 supplier
   * @param s3FileIOProperties S3 FileIO properties
   * @deprecated since 1.10.0, will be removed in 1.11.0; use {@link
   *     S3FileIO#S3FileIO(SerializableSupplier)} with {@link S3FileIO#initialize(Map)} instead
   */
  @Deprecated
  public S3FileIO(SerializableSupplier<S3Client> s3, S3FileIOProperties s3FileIOProperties) {
    this(s3, null, s3FileIOProperties);
  }

  /**
   * Constructor with custom s3 supplier, s3Async supplier and S3FileIO properties.
   *
   * <p>Calling {@link S3FileIO#initialize(Map)} will overwrite information set in this constructor.
   *
   * @param s3 s3 supplier
   * @param s3Async s3Async supplier
   * @param s3FileIOProperties S3 FileIO properties
   * @deprecated since 1.10.0, will be removed in 1.11.0; use {@link
   *     S3FileIO#S3FileIO(SerializableSupplier, SerializableSupplier)} with {@link
   *     S3FileIO#initialize(Map)} instead
   */
  @Deprecated
  public S3FileIO(
      SerializableSupplier<S3Client> s3,
      SerializableSupplier<S3AsyncClient> s3Async,
      S3FileIOProperties s3FileIOProperties) {
    this.s3 = s3;
    this.s3Async = s3Async;
    this.createStack = Thread.currentThread().getStackTrace();
    this.properties = SerializableMap.copyOf(s3FileIOProperties.properties());
  }

  @Override
  public InputFile newInputFile(String path) {
    return S3InputFile.fromLocation(path, clientForStoragePath(path), metrics);
  }

  @Override
  public InputFile newInputFile(String path, long length) {
    return S3InputFile.fromLocation(path, length, clientForStoragePath(path), metrics);
  }

  @Override
  public OutputFile newOutputFile(String path) {
    return S3OutputFile.fromLocation(path, clientForStoragePath(path), metrics);
  }

  @Override
  public void deleteFile(String path) {
    PrefixedS3Client client = clientForStoragePath(path);
    S3FileIOProperties s3FileIOProperties = client.s3FileIOProperties();
    if (s3FileIOProperties.deleteTags() != null && !s3FileIOProperties.deleteTags().isEmpty()) {
      try {
        tagFileToDelete(client, path, s3FileIOProperties.deleteTags());
      } catch (S3Exception e) {
        LOG.warn("Failed to add delete tags: {} to {}", s3FileIOProperties.deleteTags(), path, e);
      }
    }

    if (!s3FileIOProperties.isDeleteEnabled()) {
      return;
    }

    S3URI location = new S3URI(path, s3FileIOProperties.bucketToAccessPointMapping());
    DeleteObjectRequest deleteRequest =
        DeleteObjectRequest.builder().bucket(location.bucket()).key(location.key()).build();

    client.s3().deleteObject(deleteRequest);
  }

  @Override
  public Map<String, String> properties() {
    return properties.immutableMap();
  }

  /**
   * Deletes the given paths in a batched manner.
   *
   * <p>The paths are grouped by bucket, and deletion is triggered when we either reach the
   * configured batch size or have a final remainder batch for each bucket.
   *
   * @param paths paths to delete
   */
  @Override
  public void deleteFiles(Iterable<String> paths) throws BulkDeletionFailureException {
    S3FileIOProperties s3FileIOProperties = clientForStoragePath(ROOT_PREFIX).s3FileIOProperties();
    if (s3FileIOProperties.deleteTags() != null && !s3FileIOProperties.deleteTags().isEmpty()) {
      Tasks.foreach(paths)
          .noRetry()
          .executeWith(executorService())
          .suppressFailureWhenFinished()
          .onFailure(
              (path, exc) ->
                  LOG.warn(
                      "Failed to add delete tags: {} to {}",
                      s3FileIOProperties.deleteTags(),
                      path,
                      exc))
          .run(
              path ->
                  tagFileToDelete(
                      clientForStoragePath(path), path, s3FileIOProperties.deleteTags()));
    }

    if (s3FileIOProperties.isDeleteEnabled()) {
      SetMultimap<String, String> bucketToObjects =
          Multimaps.newSetMultimap(Maps.newHashMap(), Sets::newHashSet);
      List<Future<List<String>>> deletionTasks = Lists.newArrayList();
      for (String path : paths) {
        PrefixedS3Client client = clientForStoragePath(path);
        S3URI location = new S3URI(path, client.s3FileIOProperties().bucketToAccessPointMapping());
        String bucket = location.bucket();
        String objectKey = location.key();
        bucketToObjects.get(bucket).add(objectKey);
        if (bucketToObjects.get(bucket).size() == client.s3FileIOProperties().deleteBatchSize()) {
          Set<String> keys = Sets.newHashSet(bucketToObjects.get(bucket));
          Future<List<String>> deletionTask =
              executorService().submit(() -> deleteBatch(client, bucket, keys));
          deletionTasks.add(deletionTask);
          bucketToObjects.removeAll(bucket);
        }
      }

      // Delete the remainder
      for (Map.Entry<String, Collection<String>> bucketToObjectsEntry :
          bucketToObjects.asMap().entrySet()) {
        String bucket = bucketToObjectsEntry.getKey();
        Collection<String> keys = bucketToObjectsEntry.getValue();
        Future<List<String>> deletionTask =
            executorService()
                .submit(() -> deleteBatch(clientForStoragePath("s3://" + bucket), bucket, keys));
        deletionTasks.add(deletionTask);
      }

      int totalFailedDeletions = 0;

      for (Future<List<String>> deletionTask : deletionTasks) {
        try {
          List<String> failedDeletions = deletionTask.get();
          failedDeletions.forEach(path -> LOG.warn("Failed to delete object at path {}", path));
          totalFailedDeletions += failedDeletions.size();
        } catch (ExecutionException e) {
          LOG.warn("Caught unexpected exception during batch deletion: ", e.getCause());
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          deletionTasks.stream().filter(task -> !task.isDone()).forEach(task -> task.cancel(true));
          throw new RuntimeException("Interrupted when waiting for deletions to complete", e);
        }
      }

      if (totalFailedDeletions > 0) {
        throw new BulkDeletionFailureException(totalFailedDeletions);
      }
    }
  }

  private void tagFileToDelete(PrefixedS3Client client, String path, Set<Tag> deleteTags)
      throws S3Exception {
    S3URI location = new S3URI(path, client.s3FileIOProperties().bucketToAccessPointMapping());
    String bucket = location.bucket();
    String objectKey = location.key();
    GetObjectTaggingRequest getObjectTaggingRequest =
        GetObjectTaggingRequest.builder().bucket(bucket).key(objectKey).build();
    GetObjectTaggingResponse getObjectTaggingResponse =
        client.s3().getObjectTagging(getObjectTaggingRequest);
    // Get existing tags, if any and then add the delete tags
    Set<Tag> tags = Sets.newHashSet();
    if (getObjectTaggingResponse.hasTagSet()) {
      tags.addAll(getObjectTaggingResponse.tagSet());
    }

    tags.addAll(deleteTags);
    PutObjectTaggingRequest putObjectTaggingRequest =
        PutObjectTaggingRequest.builder()
            .bucket(bucket)
            .key(objectKey)
            .tagging(Tagging.builder().tagSet(tags).build())
            .build();
    client.s3().putObjectTagging(putObjectTaggingRequest);
  }

  private List<String> deleteBatch(
      PrefixedS3Client client, String bucket, Collection<String> keysToDelete) {
    List<ObjectIdentifier> objectIds =
        keysToDelete.stream()
            .map(key -> ObjectIdentifier.builder().key(key).build())
            .collect(Collectors.toList());
    DeleteObjectsRequest request =
        DeleteObjectsRequest.builder()
            .bucket(bucket)
            .delete(Delete.builder().objects(objectIds).build())
            .build();
    List<String> failures = Lists.newArrayList();
    try {
      DeleteObjectsResponse response = client.s3().deleteObjects(request);
      if (response.hasErrors()) {
        failures.addAll(
            response.errors().stream()
                .map(error -> String.format("s3://%s/%s", request.bucket(), error.key()))
                .collect(Collectors.toList()));
      }
    } catch (Exception e) {
      LOG.warn("Encountered failure when deleting batch", e);
      failures.addAll(
          request.delete().objects().stream()
              .map(obj -> String.format("s3://%s/%s", request.bucket(), obj.key()))
              .collect(Collectors.toList()));
    }
    return failures;
  }

  @Override
  public Iterable<FileInfo> listPrefix(String prefix) {
    PrefixedS3Client client = clientForStoragePath(prefix);

    S3URI uri = new S3URI(prefix, client.s3FileIOProperties().bucketToAccessPointMapping());
    if (uri.useS3DirectoryBucket()
        && client.s3FileIOProperties().isS3DirectoryBucketListPrefixAsDirectory()) {
      uri = uri.toDirectoryPath();
    }

    S3URI s3uri = uri;
    ListObjectsV2Request request =
        ListObjectsV2Request.builder().bucket(s3uri.bucket()).prefix(s3uri.key()).build();

    return () ->
        client.s3().listObjectsV2Paginator(request).stream()
            .flatMap(r -> r.contents().stream())
            .map(
                o ->
                    new FileInfo(
                        String.format("%s://%s/%s", s3uri.scheme(), s3uri.bucket(), o.key()),
                        o.size(),
                        o.lastModified().toEpochMilli()))
            .iterator();
  }

  /**
   * This method provides a "best-effort" to delete all objects under the given prefix.
   *
   * <p>Bulk delete operations are used and no reattempt is made for deletes if they fail, but will
   * log any individual objects that are not deleted as part of the bulk operation.
   *
   * @param prefix prefix to delete
   */
  @Override
  public void deletePrefix(String prefix) {
    deleteFiles(() -> Streams.stream(listPrefix(prefix)).map(FileInfo::location).iterator());
  }

  public S3Client client() {
    return client(ROOT_PREFIX);
  }

  @SuppressWarnings("resource")
  public S3Client client(String storagePath) {
    return clientForStoragePath(storagePath).s3();
  }

  public S3AsyncClient asyncClient() {
    return asyncClient(ROOT_PREFIX);
  }

  @SuppressWarnings("resource")
  public S3AsyncClient asyncClient(String storagePath) {
    return clientForStoragePath(storagePath).s3Async();
  }

  @VisibleForTesting
  PrefixedS3Client clientForStoragePath(String storagePath) {
    PrefixedS3Client client;
    String matchingPrefix = ROOT_PREFIX;

    for (String storagePrefix : clientByPrefix().keySet()) {
      if (storagePath.startsWith(storagePrefix)
          && storagePrefix.length() > matchingPrefix.length()) {
        matchingPrefix = storagePrefix;
      }
    }

    client = clientByPrefix().getOrDefault(matchingPrefix, null);

    Preconditions.checkState(
        null != client, "[BUG] S3 client for storage path not available: %s", storagePath);
    return client;
  }

  private Map<String, PrefixedS3Client> clientByPrefix() {
    if (null == clientByPrefix) {
      synchronized (this) {
        if (null == clientByPrefix) {
          Map<String, PrefixedS3Client> localClientByPrefix = Maps.newHashMap();

          localClientByPrefix.put(
              ROOT_PREFIX, new PrefixedS3Client(ROOT_PREFIX, properties, s3, s3Async));
          storageCredentials.stream()
              .filter(c -> c.prefix().startsWith(ROOT_PREFIX))
              .collect(Collectors.toList())
              .forEach(
                  storageCredential -> {
                    Map<String, String> propertiesWithCredentials =
                        ImmutableMap.<String, String>builder()
                            .putAll(properties)
                            .putAll(storageCredential.config())
                            .buildKeepingLast();

                    localClientByPrefix.put(
                        storageCredential.prefix(),
                        new PrefixedS3Client(
                            storageCredential.prefix(), propertiesWithCredentials, s3, s3Async));
                  });
          this.clientByPrefix = localClientByPrefix;
        }
      }
    }

    return clientByPrefix;
  }

  private ExecutorService executorService() {
    if (executorService == null) {
      synchronized (S3FileIO.class) {
        if (executorService == null) {
          executorService =
              ThreadPools.newExitingWorkerPool(
                  "iceberg-s3fileio-delete",
                  clientForStoragePath(ROOT_PREFIX).s3FileIOProperties().deleteThreads());
        }
      }
    }

    return executorService;
  }

  @Override
  public String getCredential() {
    return credential;
  }

  @Override
  public void initialize(Map<String, String> props) {
    this.properties = SerializableMap.copyOf(props);

    this.createStack =
        PropertyUtil.propertyAsBoolean(properties, "init-creation-stacktrace", true)
            ? Thread.currentThread().getStackTrace()
            : null;

    if (s3 == null) {
      Object clientFactory = S3FileIOAwsClientFactories.initialize(properties);
      if (clientFactory instanceof CredentialSupplier) {
        this.credential = ((CredentialSupplier) clientFactory).getCredential();
      }
    }

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
      MetricsContext context = ctor.newInstance(ROOT_PREFIX);
      context.initialize(props);
      this.metrics = context;
    } catch (NoClassDefFoundError | NoSuchMethodException | ClassCastException e) {
      LOG.warn(
          "Unable to load metrics class: '{}', falling back to null metrics", DEFAULT_METRICS_IMPL);
    }
  }

  @Override
  public void close() {
    // handles concurrent calls to close()
    if (isResourceClosed.compareAndSet(false, true)) {
      if (clientByPrefix != null) {
        clientByPrefix.values().forEach(PrefixedS3Client::close);
        this.clientByPrefix = null;
      }
    }
  }

  @SuppressWarnings({"checkstyle:NoFinalizer", "Finalize"})
  @Override
  protected void finalize() throws Throwable {
    super.finalize();
    if (!isResourceClosed.get()) {
      close();

      if (null != createStack) {
        String trace =
            Joiner.on("\n\t").join(Arrays.copyOfRange(createStack, 1, createStack.length));
        LOG.warn("Unclosed S3FileIO instance created by:\n\t{}", trace);
      }
    }
  }

  @Override
  public boolean recoverFile(String path) {
    PrefixedS3Client client = clientForStoragePath(path);
    S3URI location = new S3URI(path, client.s3FileIOProperties().bucketToAccessPointMapping());
    ListObjectVersionsIterable response =
        client
            .s3()
            .listObjectVersionsPaginator(
                builder -> builder.bucket(location.bucket()).prefix(location.key()));

    // Recover to the last modified version, not isLatest,
    // since isLatest is true for deletion markers.
    Optional<ObjectVersion> recoverVersion =
        response.versions().stream().max(Comparator.comparing(ObjectVersion::lastModified));

    return recoverVersion
        .map(version -> recoverObject(client, version, location.bucket()))
        .orElse(false);
  }

  private boolean recoverObject(PrefixedS3Client client, ObjectVersion version, String bucket) {
    if (version.isLatest()) {
      return true;
    }

    LOG.info("Attempting to recover object {}", version.key());
    try {
      // Perform a copy instead of deleting the delete marker
      // so that recovery does not rely on delete permissions
      client
          .s3()
          .copyObject(
              builder ->
                  builder
                      .sourceBucket(bucket)
                      .sourceKey(version.key())
                      .sourceVersionId(version.versionId())
                      .destinationBucket(bucket)
                      .destinationKey(version.key()));
    } catch (SdkException e) {
      LOG.warn("Failed to recover object {}", version.key(), e);
      return false;
    }

    return true;
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
