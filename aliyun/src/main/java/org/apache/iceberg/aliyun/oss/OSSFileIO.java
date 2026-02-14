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
package org.apache.iceberg.aliyun.oss;

import com.aliyun.oss.ClientException;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.DeleteObjectsRequest;
import com.aliyun.oss.model.DeleteObjectsResult;
import com.aliyun.oss.model.ListObjectsV2Request;
import com.aliyun.oss.model.ListObjectsV2Result;
import com.aliyun.oss.model.OSSObjectSummary;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.apache.iceberg.aliyun.AliyunClientFactories;
import org.apache.iceberg.aliyun.AliyunClientFactory;
import org.apache.iceberg.aliyun.AliyunProperties;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.io.BulkDeletionFailureException;
import org.apache.iceberg.io.DelegateFileIO;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.metrics.MetricsContext;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
import org.apache.iceberg.util.SerializableSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FileIO implementation backed by OSS.
 *
 * <p>Locations used must follow the conventions for OSS URIs (e.g. oss://bucket/path...). URIs with
 * scheme https are also treated as oss file paths. Using this FileIO with other schemes with result
 * in {@link org.apache.iceberg.exceptions.ValidationException}
 */
public class OSSFileIO implements DelegateFileIO {
  private static final Logger LOG = LoggerFactory.getLogger(OSSFileIO.class);
  private static final String DEFAULT_METRICS_IMPL =
      "org.apache.iceberg.hadoop.HadoopMetricsContext";

  private SerializableSupplier<OSS> oss;
  private AliyunProperties aliyunProperties;
  private transient volatile OSS client;
  private MetricsContext metrics = MetricsContext.nullMetrics();
  private final AtomicBoolean isResourceClosed = new AtomicBoolean(false);

  /**
   * No-arg constructor to load the FileIO dynamically.
   *
   * <p>All fields are initialized by calling {@link OSSFileIO#initialize(Map)} later.
   */
  public OSSFileIO() {}

  /**
   * Constructor with custom oss supplier and default aliyun properties.
   *
   * <p>Calling {@link OSSFileIO#initialize(Map)} will overwrite information set in this
   * constructor.
   *
   * @param oss oss supplier
   */
  public OSSFileIO(SerializableSupplier<OSS> oss) {
    this.oss = oss;
    this.aliyunProperties = new AliyunProperties();
  }

  /**
   * Constructor with custom oss supplier and custom aliyun properties.
   *
   * <p>Calling {@link OSSFileIO#initialize(Map)} will overwrite information set in this
   * constructor.
   *
   * @param oss oss supplier
   * @param aliyunProperties aliyun properties
   */
  public OSSFileIO(SerializableSupplier<OSS> oss, AliyunProperties aliyunProperties) {
    this.oss = oss;
    this.aliyunProperties = aliyunProperties;
  }

  @Override
  public InputFile newInputFile(String path) {
    return new OSSInputFile(client(), new OSSURI(path), aliyunProperties, metrics);
  }

  @Override
  public OutputFile newOutputFile(String path) {
    return new OSSOutputFile(client(), new OSSURI(path), aliyunProperties, metrics);
  }

  @Override
  public void deleteFile(String path) {
    OSSURI location = new OSSURI(path);
    client().deleteObject(location.bucket(), location.key());
  }

  private OSS client() {
    if (client == null) {
      synchronized (this) {
        if (client == null) {
          client = oss.get();
        }
      }
    }
    return client;
  }

  @Override
  public void initialize(Map<String, String> properties) {
    AliyunClientFactory factory = AliyunClientFactories.from(properties);
    this.aliyunProperties = factory.aliyunProperties();
    this.oss = factory::newOSSClient;

    // Report Hadoop metrics if Hadoop is available
    try {
      DynConstructors.Ctor<MetricsContext> ctor =
          DynConstructors.builder(MetricsContext.class)
              .hiddenImpl(DEFAULT_METRICS_IMPL, String.class)
              .buildChecked();
      MetricsContext context = ctor.newInstance("oss");
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
        client.shutdown();
      }
    }
  }

  /**
   * Return an iterable of all files under a prefix.
   *
   * <p>This method uses the OSS ListObjectsV2 API to retrieve objects in a paginated manner. It
   * automatically handles continuation tokens to fetch all pages of results.
   *
   * @param prefix prefix to list (e.g. "oss://bucket/path/")
   * @return iterable of file information
   */
  @Override
  public Iterable<FileInfo> listPrefix(String prefix) {
    OSSURI uri = new OSSURI(prefix);
    return new OSSListObjectV2Iterable(client(), uri);
  }

  /**
   * Delete all files under a prefix.
   *
   * <p>This method first lists all files under the given prefix using {@link #listPrefix(String)},
   * then delegates to {@link #deleteFiles(Iterable)} to perform the actual deletion. The deletion
   * is performed in batches according to the configured batch size.
   *
   * @param prefix prefix to delete (e.g. "oss://bucket/path/")
   */
  @Override
  public void deletePrefix(String prefix) {
    deleteFiles(() -> Streams.stream(listPrefix(prefix)).map(FileInfo::location).iterator());
  }

  /**
   * Delete files in the given path iterable.
   *
   * <p>Files are grouped by bucket first, then within each bucket, they are processed in batches of
   * the configured size. This approach minimizes the number of API calls while respecting OSS API
   * limits. The batch size is controlled by the {@link AliyunProperties#OSS_DELETE_BATCH_SIZE}
   * property, which defaults to 1000.
   *
   * <p>To configure the batch size, set the property when creating the FileIO:
   *
   * <pre>{@code
   * Map<String, String> properties = Map.of(AliyunProperties.OSS_DELETE_BATCH_SIZE, "500");
   * FileIO fileIO = CatalogUtil.loadFileIO("org.apache.iceberg.aliyun.oss.OSSFileIO", properties, conf);
   * }</pre>
   *
   * @param pathsToDelete The paths to delete
   * @throws BulkDeletionFailureException if file deletion fails
   */
  @Override
  public void deleteFiles(Iterable<String> pathsToDelete) throws BulkDeletionFailureException {
    Map<String, List<OSSURI>> bucketToKeys = Maps.newHashMap();
    for (String path : pathsToDelete) {
      OSSURI uri = new OSSURI(path);
      bucketToKeys.computeIfAbsent(uri.bucket(), k -> Lists.newArrayList()).add(uri);
    }
    int batchSize = aliyunProperties.ossDeleteBatchSize();
    List<String> failedDeletions = Lists.newArrayList();
    for (Map.Entry<String, List<OSSURI>> entry : bucketToKeys.entrySet()) {
      String bucket = entry.getKey();
      List<OSSURI> uris = entry.getValue();
      // delete a batch at a time
      for (int i = 0; i < uris.size(); i += batchSize) {
        int endIndex = Math.min(i + batchSize, uris.size());
        List<OSSURI> batchUris = uris.subList(i, endIndex);
        List<String> keys = batchUris.stream().map(OSSURI::key).collect(Collectors.toList());
        try {
          DeleteObjectsRequest deleteRequest = new DeleteObjectsRequest(bucket);
          deleteRequest.setKeys(keys);
          DeleteObjectsResult result = client().deleteObjects(deleteRequest);
          if (result.getDeletedObjects().size() != keys.size()) {
            for (OSSURI uri : batchUris) {
              if (!result.getDeletedObjects().contains(uri.key())) {
                failedDeletions.add(String.format("%s://%s/%s", uri.scheme(), bucket, uri.key()));
              }
            }
          }
        } catch (OSSException | ClientException e) {
          for (OSSURI uri : batchUris) {
            failedDeletions.add(String.format("%s://%s/%s", uri.scheme(), bucket, uri.key()));
          }
        }
      }
    }
    if (!failedDeletions.isEmpty()) {
      throw new BulkDeletionFailureException(failedDeletions.size());
    }
  }

  private static class OSSListObjectV2Iterable implements Iterable<FileInfo> {
    private final OSS client;
    private final OSSURI uri;

    OSSListObjectV2Iterable(OSS client, OSSURI uri) {
      this.client = client;
      this.uri = uri;
    }

    @Override
    @Nonnull
    public Iterator<FileInfo> iterator() {
      return new OSSListObjectV2Iterator(client, uri);
    }
  }

  private static class OSSListObjectV2Iterator implements Iterator<FileInfo> {
    private final OSS client;
    private final String scheme;
    private final String bucket;
    private final String prefix;
    private List<OSSObjectSummary> currentPage;
    private int currentIndexInPage;
    private String continuationToken;
    private boolean hasMore = true;

    OSSListObjectV2Iterator(OSS client, OSSURI uri) {
      this.client = client;
      this.scheme = uri.scheme();
      this.bucket = uri.bucket();
      this.prefix = uri.key();
      this.currentPage = Lists.newArrayList();
      this.currentIndexInPage = 0;
      fetchNextPage();
    }

    @Override
    public boolean hasNext() {
      if (currentIndexInPage < currentPage.size()) {
        return true;
      }
      if (hasMore) {
        fetchNextPage();
        return currentIndexInPage < currentPage.size();
      }
      return false;
    }

    @Override
    public FileInfo next() {
      if (!hasNext()) {
        throw new java.util.NoSuchElementException();
      }
      OSSObjectSummary summary = currentPage.get(currentIndexInPage++);
      String location = String.format("%s://%s/%s", scheme, bucket, summary.getKey());
      return new FileInfo(
          location, summary.getSize(), summary.getLastModified().toInstant().toEpochMilli());
    }

    private void fetchNextPage() {
      if (!hasMore) {
        return;
      }
      ListObjectsV2Request request = new ListObjectsV2Request(bucket).withPrefix(prefix);
      if (continuationToken != null) {
        request.setContinuationToken(continuationToken);
      }
      ListObjectsV2Result result = client.listObjectsV2(request);
      currentPage = result.getObjectSummaries();
      currentIndexInPage = 0;
      continuationToken = result.getNextContinuationToken();
      hasMore = result.isTruncated();
    }
  }
}
