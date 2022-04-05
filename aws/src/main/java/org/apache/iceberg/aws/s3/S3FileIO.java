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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.iceberg.aws.AwsClientFactories;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.io.BulkDeletionFailureException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SupportsBulkOperations;
import org.apache.iceberg.metrics.MetricsContext;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Multimaps;
import org.apache.iceberg.relocated.com.google.common.collect.SetMultimap;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
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
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectTaggingRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.Tag;
import software.amazon.awssdk.services.s3.model.Tagging;

/**
 * FileIO implementation backed by S3.
 * <p>
 * Locations used must follow the conventions for S3 URIs (e.g. s3://bucket/path...).
 * URIs with schemes s3a, s3n, https are also treated as s3 file paths.
 * Using this FileIO with other schemes will result in {@link org.apache.iceberg.exceptions.ValidationException}.
 */
public class S3FileIO implements FileIO, SupportsBulkOperations {
  private static final Logger LOG = LoggerFactory.getLogger(S3FileIO.class);
  private static final String DEFAULT_METRICS_IMPL = "org.apache.iceberg.hadoop.HadoopMetricsContext";
  private static volatile ExecutorService executorService;

  private SerializableSupplier<S3Client> s3;
  private AwsProperties awsProperties;
  private transient volatile S3Client client;
  private MetricsContext metrics = MetricsContext.nullMetrics();
  private final AtomicBoolean isResourceClosed = new AtomicBoolean(false);

  /**
   * No-arg constructor to load the FileIO dynamically.
   * <p>
   * All fields are initialized by calling {@link S3FileIO#initialize(Map)} later.
   */
  public S3FileIO() {
  }

  /**
   * Constructor with custom s3 supplier and default AWS properties.
   * <p>
   * Calling {@link S3FileIO#initialize(Map)} will overwrite information set in this constructor.
   * @param s3 s3 supplier
   */
  public S3FileIO(SerializableSupplier<S3Client> s3) {
    this(s3, new AwsProperties());
  }

  /**
   * Constructor with custom s3 supplier and AWS properties.
   * <p>
   * Calling {@link S3FileIO#initialize(Map)} will overwrite information set in this constructor.
   * @param s3 s3 supplier
   * @param awsProperties aws properties
   */
  public S3FileIO(SerializableSupplier<S3Client> s3, AwsProperties awsProperties) {
    this.s3 = s3;
    this.awsProperties = awsProperties;
  }

  @Override
  public InputFile newInputFile(String path) {
    return S3InputFile.fromLocation(path, client(), awsProperties, metrics);
  }

  @Override
  public OutputFile newOutputFile(String path) {
    return S3OutputFile.fromLocation(path, client(), awsProperties, metrics);
  }

  @Override
  public void deleteFile(String path) {
    if (awsProperties.s3DeleteTags() != null && !awsProperties.s3DeleteTags().isEmpty()) {
      try {
        tagFileToDelete(path, awsProperties.s3DeleteTags());
      } catch (S3Exception e) {
        LOG.warn("Failed to add delete tags: {} to {}", awsProperties.s3DeleteTags(), path, e);
      }
    }

    if (!awsProperties.isS3DeleteEnabled()) {
      return;
    }

    S3URI location = new S3URI(path, awsProperties.s3BucketToAccessPointMapping());
    DeleteObjectRequest deleteRequest =
        DeleteObjectRequest.builder().bucket(location.bucket()).key(location.key()).build();

    client().deleteObject(deleteRequest);
  }

  /**
   * Deletes the given paths in a batched manner.
   * <p>
   * The paths are grouped by bucket, and deletion is triggered when we either reach the configured batch size
   * or have a final remainder batch for each bucket.
   *
   * @param paths paths to delete
   */
  @Override
  public void deleteFiles(Iterable<String> paths) throws BulkDeletionFailureException {
    if (awsProperties.s3DeleteTags() != null && !awsProperties.s3DeleteTags().isEmpty()) {
      Tasks.foreach(paths)
          .noRetry()
          .executeWith(executorService())
          .suppressFailureWhenFinished()
          .onFailure((path, exc) -> LOG.warn("Failed to add delete tags: {} to {}",
              awsProperties.s3DeleteTags(), path, exc))
          .run(path -> tagFileToDelete(path, awsProperties.s3DeleteTags()));
    }

    if (!awsProperties.isS3DeleteEnabled()) {
      return;
    }

    SetMultimap<String, String> bucketToObjects = Multimaps.newSetMultimap(Maps.newHashMap(), Sets::newHashSet);
    int numberOfFailedDeletions = 0;
    for (String path : paths) {
      S3URI location = new S3URI(path, awsProperties.s3BucketToAccessPointMapping());
      String bucket = location.bucket();
      String objectKey = location.key();
      Set<String> objectsInBucket = bucketToObjects.get(bucket);
      if (objectsInBucket.size() == awsProperties.s3FileIoDeleteBatchSize()) {
        List<String> failedDeletionsForBatch = deleteObjectsInBucket(bucket, objectsInBucket);
        numberOfFailedDeletions += failedDeletionsForBatch.size();
        failedDeletionsForBatch.forEach(failedPath -> LOG.warn("Failed to delete object at path {}", failedPath));
        bucketToObjects.removeAll(bucket);
      }
      bucketToObjects.get(bucket).add(objectKey);
    }

    // Delete the remainder
    for (Map.Entry<String, Collection<String>> bucketToObjectsEntry : bucketToObjects.asMap().entrySet()) {
      final String bucket = bucketToObjectsEntry.getKey();
      final Collection<String> objects = bucketToObjectsEntry.getValue();
      List<String> failedDeletions = deleteObjectsInBucket(bucket, objects);
      failedDeletions.forEach(failedPath -> LOG.warn("Failed to delete object at path {}", failedPath));
      numberOfFailedDeletions += failedDeletions.size();
    }

    if (numberOfFailedDeletions > 0) {
      throw new BulkDeletionFailureException(numberOfFailedDeletions);
    }
  }

  private void tagFileToDelete(String path, Set<Tag> deleteTags) throws S3Exception {
    S3URI location = new S3URI(path, awsProperties.s3BucketToAccessPointMapping());
    String bucket = location.bucket();
    String objectKey = location.key();
    GetObjectTaggingRequest getObjectTaggingRequest = GetObjectTaggingRequest.builder()
        .bucket(bucket)
        .key(objectKey)
        .build();
    GetObjectTaggingResponse getObjectTaggingResponse = client()
        .getObjectTagging(getObjectTaggingRequest);
    // Get existing tags, if any and then add the delete tags
    Set<Tag> tags = Sets.newHashSet();
    if (getObjectTaggingResponse.hasTagSet()) {
      tags.addAll(getObjectTaggingResponse.tagSet());
    }

    tags.addAll(deleteTags);
    PutObjectTaggingRequest putObjectTaggingRequest = PutObjectTaggingRequest.builder()
        .bucket(bucket)
        .key(objectKey)
        .tagging(Tagging.builder().tagSet(tags).build())
        .build();
    client().putObjectTagging(putObjectTaggingRequest);
  }

  private List<String> deleteObjectsInBucket(String bucket, Collection<String> objects) {
    if (!objects.isEmpty()) {
      List<ObjectIdentifier> objectIds = objects
          .stream()
          .map(objectKey -> ObjectIdentifier.builder().key(objectKey).build())
          .collect(Collectors.toList());
      DeleteObjectsRequest deleteObjectsRequest = DeleteObjectsRequest.builder()
          .bucket(bucket)
          .delete(Delete.builder().objects(objectIds).build())
          .build();
      DeleteObjectsResponse response = client().deleteObjects(deleteObjectsRequest);
      if (response.hasErrors()) {
        return response.errors()
            .stream()
            .map(error -> String.format("s3://%s/%s", bucket, error.key()))
            .collect(Collectors.toList());
      }
    }

    return Lists.newArrayList();
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
          executorService = ThreadPools.newWorkerPool(
              "iceberg-s3fileio-delete", awsProperties.s3FileIoDeleteThreads());
        }
      }
    }

    return executorService;
  }

  @Override
  public void initialize(Map<String, String> properties) {
    this.awsProperties = new AwsProperties(properties);

    // Do not override s3 client if it was provided
    if (s3 == null) {
      this.s3 = AwsClientFactories.from(properties)::s3;
    }

    // Report Hadoop metrics if Hadoop is available
    try {
      DynConstructors.Ctor<MetricsContext> ctor =
          DynConstructors.builder(MetricsContext.class).hiddenImpl(DEFAULT_METRICS_IMPL, String.class).buildChecked();
      MetricsContext context = ctor.newInstance("s3");
      context.initialize(properties);
      this.metrics = context;
    } catch (NoClassDefFoundError | NoSuchMethodException | ClassCastException e) {
      LOG.warn("Unable to load metrics class: '{}', falling back to null metrics", DEFAULT_METRICS_IMPL, e);
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
