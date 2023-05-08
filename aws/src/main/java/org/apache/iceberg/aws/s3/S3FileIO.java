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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.iceberg.aws.AwsClientFactories;
import org.apache.iceberg.aws.AwsClientFactory;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.io.BulkDeletionFailureException;
import org.apache.iceberg.io.CredentialSupplier;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SupportsBulkOperations;
import org.apache.iceberg.io.SupportsPrefixOperations;
import org.apache.iceberg.metrics.MetricsContext;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Multimaps;
import org.apache.iceberg.relocated.com.google.common.collect.SetMultimap;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
import org.apache.iceberg.util.SerializableMap;
import org.apache.iceberg.util.SerializableSupplier;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.GetObjectTaggingRequest;
import software.amazon.awssdk.services.s3.model.GetObjectTaggingResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectTaggingRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.Tag;
import software.amazon.awssdk.services.s3.model.Tagging;

/**
 * FileIO implementation backed by S3.
 *
 * <p>Locations used must follow the conventions for S3 URIs (e.g. s3://bucket/path...). URIs with
 * schemes s3a, s3n, https are also treated as s3 file paths. Using this FileIO with other schemes
 * will result in {@link org.apache.iceberg.exceptions.ValidationException}.
 */
public class S3FileIO
    implements FileIO, SupportsBulkOperations, SupportsPrefixOperations, CredentialSupplier {
  private static final Logger LOG = LoggerFactory.getLogger(S3FileIO.class);
  private static final String DEFAULT_METRICS_IMPL =
      "org.apache.iceberg.hadoop.HadoopMetricsContext";
  private static volatile ExecutorService executorService;

  private String credential = null;
  private SerializableSupplier<S3Client> s3;
  private S3FileIOProperties s3FileIOProperties;
  private SerializableMap<String, String> properties = null;
  private transient volatile S3Client client;
  private MetricsContext metrics = MetricsContext.nullMetrics();
  private final AtomicBoolean isResourceClosed = new AtomicBoolean(false);
  private final transient StackTraceElement[] createStack;

  /**
   * No-arg constructor to load the FileIO dynamically.
   *
   * <p>All fields are initialized by calling {@link S3FileIO#initialize(Map)} later.
   */
  public S3FileIO() {
    this.createStack = Thread.currentThread().getStackTrace();
  }

  /**
   * Constructor with custom s3 supplier and S3FileIO properties.
   *
   * <p>Calling {@link S3FileIO#initialize(Map)} will overwrite information set in this constructor.
   *
   * @param s3 s3 supplier
   */
  public S3FileIO(SerializableSupplier<S3Client> s3) {
    this(s3, new S3FileIOProperties());
  }

  /**
   * Constructor with custom s3 supplier and S3FileIO properties.
   *
   * <p>Calling {@link S3FileIO#initialize(Map)} will overwrite information set in this constructor.
   *
   * @param s3 s3 supplier
   * @param s3FileIOProperties S3 FileIO properties
   */
  public S3FileIO(SerializableSupplier<S3Client> s3, S3FileIOProperties s3FileIOProperties) {
    this.s3 = s3;
    this.s3FileIOProperties = s3FileIOProperties;
    this.createStack = Thread.currentThread().getStackTrace();
  }

  @Override
  public InputFile newInputFile(String path) {
    return S3InputFile.fromLocation(path, client(), s3FileIOProperties, metrics);
  }

  @Override
  public InputFile newInputFile(String path, long length) {
    return S3InputFile.fromLocation(path, length, client(), s3FileIOProperties, metrics);
  }

  @Override
  public OutputFile newOutputFile(String path) {
    return S3OutputFile.fromLocation(path, client(), s3FileIOProperties, metrics);
  }

  @Override
  public void deleteFile(String path) {
    if (s3FileIOProperties.deleteTags() != null && !s3FileIOProperties.deleteTags().isEmpty()) {
      try {
        tagFileToDelete(path, s3FileIOProperties.deleteTags());
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

    client().deleteObject(deleteRequest);
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
          .run(path -> tagFileToDelete(path, s3FileIOProperties.deleteTags()));
    }

    if (s3FileIOProperties.isDeleteEnabled()) {
      SetMultimap<String, String> bucketToObjects =
          Multimaps.newSetMultimap(Maps.newHashMap(), Sets::newHashSet);
      List<Future<List<String>>> deletionTasks = Lists.newArrayList();
      for (String path : paths) {
        S3URI location = new S3URI(path, s3FileIOProperties.bucketToAccessPointMapping());
        String bucket = location.bucket();
        String objectKey = location.key();
        bucketToObjects.get(bucket).add(objectKey);
        if (bucketToObjects.get(bucket).size() == s3FileIOProperties.deleteBatchSize()) {
          Set<String> keys = Sets.newHashSet(bucketToObjects.get(bucket));
          Future<List<String>> deletionTask =
              executorService().submit(() -> deleteBatch(bucket, keys));
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
            executorService().submit(() -> deleteBatch(bucket, keys));
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

  private void tagFileToDelete(String path, Set<Tag> deleteTags) throws S3Exception {
    S3URI location = new S3URI(path, s3FileIOProperties.bucketToAccessPointMapping());
    String bucket = location.bucket();
    String objectKey = location.key();
    GetObjectTaggingRequest getObjectTaggingRequest =
        GetObjectTaggingRequest.builder().bucket(bucket).key(objectKey).build();
    GetObjectTaggingResponse getObjectTaggingResponse =
        client().getObjectTagging(getObjectTaggingRequest);
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
    client().putObjectTagging(putObjectTaggingRequest);
  }

  private List<String> deleteBatch(String bucket, Collection<String> keysToDelete) {
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
      DeleteObjectsResponse response = client().deleteObjects(request);
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
    S3URI s3uri = new S3URI(prefix, s3FileIOProperties.bucketToAccessPointMapping());
    ListObjectsV2Request request =
        ListObjectsV2Request.builder().bucket(s3uri.bucket()).prefix(s3uri.key()).build();

    return () ->
        client().listObjectsV2Paginator(request).stream()
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

  private S3Client client() {
    if (client == null) {
      synchronized (this) {
        if (client == null) {
          client = s3.get();
        }
      }
    }
    return client;
  }

  private ExecutorService executorService() {
    if (executorService == null) {
      synchronized (S3FileIO.class) {
        if (executorService == null) {
          executorService =
              ThreadPools.newWorkerPool(
                  "iceberg-s3fileio-delete", s3FileIOProperties.deleteThreads());
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
    this.s3FileIOProperties = new S3FileIOProperties(properties);

    // Do not override s3 client if it was provided
    if (s3 == null) {
      AwsClientFactory clientFactory = AwsClientFactories.from(props);
      if (clientFactory instanceof CredentialSupplier) {
        this.credential = ((CredentialSupplier) clientFactory).getCredential();
      }
      this.s3 = clientFactory::s3;
      if (s3FileIOProperties.isPreloadClientEnabled()) {
        client();
      }
    }

    // Report Hadoop metrics if Hadoop is available
    try {
      DynConstructors.Ctor<MetricsContext> ctor =
          DynConstructors.builder(MetricsContext.class)
              .loader(S3FileIO.class.getClassLoader())
              .hiddenImpl(DEFAULT_METRICS_IMPL, String.class)
              .buildChecked();
      MetricsContext context = ctor.newInstance("s3");
      context.initialize(properties);
      this.metrics = context;
    } catch (NoClassDefFoundError | NoSuchMethodException | ClassCastException e) {
      LOG.warn(
          "Unable to load metrics class: '{}', falling back to null metrics",
          DEFAULT_METRICS_IMPL,
          e);
    }
  }

  @Override
  public void close() {
    // handles concurrent calls to close()
    if (isResourceClosed.compareAndSet(false, true)) {
      if (client != null) {
        client.close();
      }
    }
  }

  @SuppressWarnings("checkstyle:NoFinalizer")
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
}
