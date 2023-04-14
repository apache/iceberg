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

import org.apache.iceberg.aws.AwsClientFactories;
import org.apache.iceberg.aws.AwsClientFactory;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.io.BulkDeletionFailureException;
import org.apache.iceberg.io.CredentialSupplier;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.metrics.MetricsContext;
import org.apache.iceberg.relocated.com.google.common.collect.AbstractIterator;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Multimaps;
import org.apache.iceberg.relocated.com.google.common.collect.SetMultimap;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
import org.apache.iceberg.util.SerializableMap;
import org.apache.iceberg.util.SerializableSupplier;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.GetObjectTaggingRequest;
import software.amazon.awssdk.services.s3.model.GetObjectTaggingResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectTaggingRequest;
import software.amazon.awssdk.services.s3.model.PutObjectTaggingResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.Tag;
import software.amazon.awssdk.services.s3.model.Tagging;

import javax.annotation.CheckForNull;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * FileIO implementation backed by S3.
 *
 * <p>Locations used must follow the conventions for S3 URIs (e.g. s3://bucket/path...). URIs with
 * schemes s3a, s3n, https are also treated as s3 file paths. Using this FileIO with other schemes
 * will result in {@link org.apache.iceberg.exceptions.ValidationException}.
 */
public class S3AsyncFileIO extends S3FileIOBase {
  private static final Logger LOG = LoggerFactory.getLogger(S3AsyncFileIO.class);
  private static final String DEFAULT_METRICS_IMPL = "org.apache.iceberg.hadoop.HadoopMetricsContext";

  private String credential = null;
  private SerializableSupplier<S3AsyncClient> s3;
  private AwsProperties awsProperties;
  private SerializableMap<String, String> properties = null;
  private transient volatile S3AsyncClient client;
  private MetricsContext metrics = MetricsContext.nullMetrics();
  private final AtomicBoolean isResourceClosed = new AtomicBoolean(false);

  /**
   * No-arg constructor to load the FileIO dynamically.
   *
   * <p>All fields are initialized by calling {@link S3AsyncFileIO#initialize(Map)} later.
   */
  public S3AsyncFileIO() {}

  /**
   * Constructor with custom s3 supplier and default AWS properties.
   *
   * <p>Calling {@link S3AsyncFileIO#initialize(Map)} will overwrite information set in this constructor.
   *
   * @param s3 s3 supplier
   */
  public S3AsyncFileIO(SerializableSupplier<S3AsyncClient> s3) {
    this(s3, new AwsProperties());
  }

  /**
   * Constructor with custom s3 supplier and AWS properties.
   *
   * <p>Calling {@link S3AsyncFileIO#initialize(Map)} will overwrite information set in this constructor.
   *
   * @param s3 s3 supplier
   * @param awsProperties aws properties
   */
  public S3AsyncFileIO(SerializableSupplier<S3AsyncClient> s3, AwsProperties awsProperties) {
    this.s3 = s3;
    this.awsProperties = awsProperties;
  }

  @Override
  public Map<String, String> properties() {
    return properties.immutableMap();
  }

  @Override
  public InputFile newInputFile(String path) {
    return S3AsyncInputFile.fromLocation(path, client(), awsProperties, metrics);
  }

  @Override
  public InputFile newInputFile(String path, long length) {
    return S3AsyncInputFile.fromLocation(path, length, client(), awsProperties, metrics);
  }

  @Override
  public OutputFile newOutputFile(String path) {
    return S3AsyncOutputFile.fromLocation(path, client(), awsProperties, metrics);
  }

  @Override
  public void deleteFile(String path) {
    CompletableFuture<PutObjectTaggingResponse> tagFuture = CompletableFuture.completedFuture(null);
    if (awsProperties.s3DeleteTags() != null && !awsProperties.s3DeleteTags().isEmpty()) {
      tagFuture = tagFile(path, awsProperties.s3DeleteTags());
    }

    CompletableFuture<DeleteObjectResponse> deleteFuture = CompletableFuture.completedFuture(null);
    if (awsProperties.isS3DeleteEnabled()) {
      S3URI location = new S3URI(path, awsProperties.s3BucketToAccessPointMapping());
      DeleteObjectRequest deleteRequest =
          DeleteObjectRequest.builder().bucket(location.bucket()).key(location.key()).build();
      // Delete unconditionally, but after tagging is complete
      deleteFuture = tagFuture
          .exceptionally(t -> null)
          .thenCompose((ignore) -> client().deleteObject(deleteRequest));
    }

    CompletableFuture.allOf(tagFuture, deleteFuture).join();
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
    if (awsProperties.s3DeleteTags() != null && !awsProperties.s3DeleteTags().isEmpty()) {
      final Set<Tag> deleteTags = awsProperties.s3DeleteTags();
      final CompletableFuture<?>[] tagFutures = StreamSupport.stream(paths.spliterator(), false)
          .map(path -> tagFile(path, deleteTags))
          .toArray(CompletableFuture[]::new);
      // There is a potential here to further improve concurrency by combining this with deletion future.
      CompletableFuture.allOf(tagFutures).join();
    }

    if (awsProperties.isS3DeleteEnabled()) {
      SetMultimap<String, String> bucketToObjects =
          Multimaps.newSetMultimap(Maps.newHashMap(), Sets::newHashSet);
      List<CompletableFuture<List<String>>> deletionTasks = Lists.newArrayList();
      for (String path : paths) {
        S3URI location = new S3URI(path, awsProperties.s3BucketToAccessPointMapping());
        String bucket = location.bucket();
        String objectKey = location.key();
        bucketToObjects.get(bucket).add(objectKey);
        if (bucketToObjects.get(bucket).size() == awsProperties.s3FileIoDeleteBatchSize()) {
          Set<String> keys = Sets.newHashSet(bucketToObjects.get(bucket));
          CompletableFuture<List<String>> deletionTask = deleteBatch(bucket, keys);
          deletionTasks.add(deletionTask);
          bucketToObjects.removeAll(bucket);
        }
      }

      // Delete the remainder
      for (Map.Entry<String, Collection<String>> bucketToObjectsEntry :
          bucketToObjects.asMap().entrySet()) {
        String bucket = bucketToObjectsEntry.getKey();
        Collection<String> keys = bucketToObjectsEntry.getValue();
        CompletableFuture<List<String>> deletionTask = deleteBatch(bucket, keys);
        deletionTasks.add(deletionTask);
      }

      final AtomicInteger totalFailedDeletions = new AtomicInteger(0);
      final CompletableFuture<?>[] tasks = deletionTasks
          .stream()
          .map(task ->
              task.thenAccept(failures -> {
                failures.forEach(path -> LOG.warn("Failed to delete object at path {}", path));
                totalFailedDeletions.addAndGet(failures.size());
              }))
          .toArray(CompletableFuture[]::new);
      try {
        CompletableFuture.allOf(tasks).join();
      } catch (CompletionException e) {
        LOG.warn("Caught unexpected exception during batch deletion: ", e.getCause());
      } catch (CancellationException e) {
        throw new RuntimeException("Task cancelled when waiting for deletions to complete", e);
      }

      if (totalFailedDeletions.get() > 0) {
        throw new BulkDeletionFailureException(totalFailedDeletions.get());
      }
    }
  }

  /**
   * Tags files in S3. The method will always return a successful future, handling failures.
   *
   * @param path S3 Path for the file to delete
   * @param tagsToAdd tags to add for deletion
   * @return a Future that completes when the tags have been applied or null if failed.
   * @throws S3Exception
   */
  private CompletableFuture<PutObjectTaggingResponse> tagFile(String path, Set<Tag> tagsToAdd) throws S3Exception {
    S3URI location = new S3URI(path, awsProperties.s3BucketToAccessPointMapping());
    String bucket = location.bucket();
    String objectKey = location.key();
    GetObjectTaggingRequest getObjectTaggingRequest =
        GetObjectTaggingRequest.builder().bucket(bucket).key(objectKey).build();
    CompletableFuture<GetObjectTaggingResponse> responseFuture =
        client().getObjectTagging(getObjectTaggingRequest);
    return responseFuture
        .thenCompose(response -> {
          // Get existing tags, if any and then add the delete tags
          Set<Tag> tags = Sets.newHashSet(tagsToAdd);
          tags.addAll(response.tagSet());

          PutObjectTaggingRequest putObjectTaggingRequest =
              PutObjectTaggingRequest.builder()
                  .bucket(bucket)
                  .key(objectKey)
                  .tagging(Tagging.builder().tagSet(tags).build())
                  .build();
          return client().putObjectTagging(putObjectTaggingRequest);
        })
        .handle((resp, exc) -> {
          if (exc != null) {
            LOG.warn("Failed to add tags: {} to {}", tagsToAdd, path, exc);
          }
          return null;
        });
  }

  /**
   * Delete a batch of keys asynchronously.
   *
   * @return A future with failed objects
   */
  private CompletableFuture<List<String>> deleteBatch(String bucket, Collection<String> keysToDelete) {
    List<ObjectIdentifier> objectIds =
        keysToDelete.stream()
            .map(key -> ObjectIdentifier.builder().key(key).build())
            .collect(Collectors.toList());
    DeleteObjectsRequest request =
        DeleteObjectsRequest.builder()
            .bucket(bucket)
            .delete(Delete.builder().objects(objectIds).build())
            .build();
    return client().deleteObjects(request)
        .handle((response, throwable) -> {
          List<String> failures = Lists.newArrayList();
          if (response != null && response.hasErrors()) {
            failures.addAll(
                response.errors().stream()
                    .map(error -> String.format("s3://%s/%s", request.bucket(), error.key()))
                    .collect(Collectors.toList()));
          }
          if (throwable != null) {
            LOG.warn("Encountered failure when deleting batch", throwable);
            failures.addAll(
                request.delete().objects().stream()
                    .map(obj -> String.format("s3://%s/%s", request.bucket(), obj.key()))
                    .collect(Collectors.toList()));
          }
          return failures;
        });
  }

  @Override
  public Iterable<FileInfo> listPrefix(String prefix) {
    S3URI s3uri = new S3URI(prefix, awsProperties.s3BucketToAccessPointMapping());
    ListObjectsV2Request request =
        ListObjectsV2Request.builder().bucket(s3uri.bucket()).prefix(s3uri.key()).build();

    SdkPublisher<FileInfo> publisher = client().listObjectsV2Paginator(request)
        .contents()
        .map(o ->
            new FileInfo(
                String.format("%s://%s/%s", s3uri.scheme(), s3uri.bucket(), o.key()),
                o.size(),
                o.lastModified().toEpochMilli()));
    // TODO: 100 is arbitrary between 1 and 1000 (S3 page size)
    SubscriberIterator<FileInfo> subscriber = new SubscriberIterator<>(publisher, 100);
    publisher.subscribe(subscriber);
    return () -> subscriber;
  }

  private static class SubscriberIterator<T> extends AbstractIterator<T> implements Subscriber<T> {
    private final SdkPublisher<T> source;
    private final BlockingQueue<Optional<T>> items;
    private Throwable thrown;

    SubscriberIterator(SdkPublisher<T> source, int bufferSize) {
      super();
      this.source = source;
      this.items = new ArrayBlockingQueue<>(bufferSize);
    }

    private final AtomicInteger put = new AtomicInteger(0);
    private final AtomicInteger taken = new AtomicInteger(0);
    private void putInternal(Optional<T> item) {
      try {
        items.put(item);
        put.getAndIncrement();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    }

    private Optional<T> takeInternal() {
      try {
        Optional<T> taken = items.take();
        this.taken.getAndIncrement();
        return taken;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    }
    @Override
    public void onSubscribe(Subscription s) {
      s.request(Long.MAX_VALUE);
    }

    @Override
    public void onNext(T item) {
      putInternal(Optional.of(item));
    }

    @Override
    public void onError(Throwable t) {
      if (this.thrown == null) {
        this.thrown = t;
      } else {
        this.thrown.addSuppressed(t);
      }
      putInternal(Optional.empty());
      LOG.error("Error from S3", t);
    }

    @Override
    public void onComplete() {
      putInternal(Optional.empty());
    }

    @CheckForNull
    @Override
    protected T computeNext() {
      if (items.isEmpty()) {
        if (thrown != null) {
          throwUnchecked(thrown);
        }
      }
      return takeInternal().orElseGet(this::endOfData);
    }
  }

  private static <E extends Throwable> void throwUnchecked(Throwable thrown) throws E {
    throw (E) thrown;
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

  private S3AsyncClient client() {
    if (client == null) {
      synchronized (this) {
        if (client == null) {
          client = s3.get();
        }
      }
    }
    return client;
  }

  @Override
  public String getCredential() {
    return credential;
  }

  @Override
  public void initialize(Map<String, String> props) {
    this.properties = SerializableMap.copyOf(props);
    this.awsProperties = new AwsProperties(properties);

    // Do not override s3 client if it was provided
    if (s3 == null) {
      AwsClientFactory clientFactory = AwsClientFactories.from(props);
      if (clientFactory instanceof CredentialSupplier) {
        this.credential = ((CredentialSupplier) clientFactory).getCredential();
      }
      this.s3 = clientFactory::s3Async;
      if (awsProperties.s3PreloadClientEnabled()) {
        client();
      }
    }

    // Report Hadoop metrics if Hadoop is available
    try {
      DynConstructors.Ctor<MetricsContext> ctor =
          DynConstructors.builder(MetricsContext.class)
              .loader(S3AsyncFileIO.class.getClassLoader())
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
}
