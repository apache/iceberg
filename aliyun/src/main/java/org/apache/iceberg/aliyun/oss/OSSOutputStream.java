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

import com.aliyun.oss.OSS;
import com.aliyun.oss.model.AbortMultipartUploadRequest;
import com.aliyun.oss.model.CompleteMultipartUploadRequest;
import com.aliyun.oss.model.InitiateMultipartUploadRequest;
import com.aliyun.oss.model.ObjectMetadata;
import com.aliyun.oss.model.PartETag;
import com.aliyun.oss.model.PutObjectRequest;
import com.aliyun.oss.model.UploadPartRequest;
import com.aliyun.oss.model.UploadPartResult;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.iceberg.aliyun.AliyunProperties;
import org.apache.iceberg.io.FileIOMetricsContext;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.metrics.Counter;
import org.apache.iceberg.metrics.MetricsContext;
import org.apache.iceberg.metrics.MetricsContext.Unit;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.io.CountingOutputStream;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.MoreExecutors;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OSSOutputStream extends PositionOutputStream {
  private static final Logger LOG = LoggerFactory.getLogger(OSSOutputStream.class);

  private static volatile ExecutorService executorService;

  private final StackTraceElement[] createStack;
  private final OSS client;
  private final OSSURI uri;

  private final File stagingDirectory;
  private final int multiPartSize;
  private final int multiPartThresholdSize;

  private final List<File> stagingFiles = Lists.newArrayList();
  private final Map<File, CompletableFuture<PartETag>> parts = Maps.newLinkedHashMap();
  private CountingOutputStream stream;
  private File currentStagingFile;
  private String multipartUploadId;

  private long pos = 0;
  private boolean closed = false;

  private final Counter writeBytes;
  private final Counter writeOperations;

  @SuppressWarnings("StaticAssignmentInConstructor")
  OSSOutputStream(OSS client, OSSURI uri, AliyunProperties aliyunProperties, MetricsContext metrics)
      throws IOException {
    if (executorService == null) {
      synchronized (OSSOutputStream.class) {
        if (executorService == null) {
          executorService =
              MoreExecutors.getExitingExecutorService(
                  (ThreadPoolExecutor)
                      Executors.newFixedThreadPool(
                          aliyunProperties.ossMultipartUploadThreads(),
                          new ThreadFactoryBuilder()
                              .setDaemon(true)
                              .setNameFormat("iceberg-ossfileio-upload-%d")
                              .build()));
        }
      }
    }

    this.client = client;
    this.uri = uri;
    this.createStack = Thread.currentThread().getStackTrace();

    this.multiPartSize = aliyunProperties.ossMultipartSize();
    this.multiPartThresholdSize =
        (int) (multiPartSize * aliyunProperties.ossMultipartThresholdFactor());
    this.stagingDirectory = new File(aliyunProperties.ossStagingDirectory());

    this.writeBytes = metrics.counter(FileIOMetricsContext.WRITE_BYTES, Unit.BYTES);
    this.writeOperations = metrics.counter(FileIOMetricsContext.WRITE_OPERATIONS);

    newStream();
  }

  @Override
  public long getPos() {
    return pos;
  }

  @Override
  public void flush() throws IOException {
    stream.flush();
  }

  @Override
  public void write(int b) throws IOException {
    if (stream.getCount() >= multiPartSize) {
      newStream();
      uploadParts();
    }

    stream.write(b);
    pos += 1;
    writeBytes.increment();
    writeOperations.increment();

    if (multipartUploadId == null && pos >= multiPartThresholdSize) {
      initializeMultiPartUpload();
      uploadParts();
    }
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    int remaining = len;
    int relativeOffset = off;

    while (stream.getCount() + remaining > multiPartSize) {
      int writeSize = multiPartSize - (int) stream.getCount();
      stream.write(b, relativeOffset, writeSize);
      remaining -= writeSize;
      relativeOffset += writeSize;
      newStream();
      uploadParts();
    }

    stream.write(b, relativeOffset, remaining);
    pos += len;
    writeBytes.increment(len);
    writeOperations.increment();

    if (multipartUploadId == null && pos >= multiPartThresholdSize) {
      initializeMultiPartUpload();
      uploadParts();
    }
  }

  @Override
  public void close() throws IOException {
    close(true);
  }

  private void close(boolean completeUploads) throws IOException {
    if (closed) {
      return;
    }

    super.close();
    closed = true;

    try {
      stream.close();
      if (completeUploads) {
        completeUploads();
      }
    } finally {
      cleanUpStagingFiles();
    }
  }

  private void newStream() throws IOException {
    if (stream != null) {
      stream.close();
    }

    createStagingDirectoryIfNotExists();
    currentStagingFile = File.createTempFile("oss-file-io-", ".tmp", stagingDirectory);
    stagingFiles.add(currentStagingFile);
    stream =
        new CountingOutputStream(
            new BufferedOutputStream(Files.newOutputStream(currentStagingFile.toPath())));
  }

  private void initializeMultiPartUpload() {
    multipartUploadId =
        client
            .initiateMultipartUpload(new InitiateMultipartUploadRequest(uri.bucket(), uri.key()))
            .getUploadId();
  }

  private void uploadParts() {
    if (multipartUploadId == null) {
      return;
    }

    IntStream.range(0, stagingFiles.size())
        .filter(i -> closed || !stagingFiles.get(i).equals(currentStagingFile))
        .filter(i -> !parts.containsKey(stagingFiles.get(i)))
        .forEach(
            i -> {
              File stagingFile = stagingFiles.get(i);
              int partNumber = i + 1;
              UploadPartRequest request = new UploadPartRequest();
              request.setBucketName(uri.bucket());
              request.setKey(uri.key());
              request.setUploadId(multipartUploadId);
              request.setPartNumber(partNumber);
              request.setInputStream(uncheckedInputStream(stagingFile));
              request.setPartSize(stagingFile.length());

              CompletableFuture<PartETag> future =
                  CompletableFuture.supplyAsync(
                          () -> {
                            UploadPartResult result = client.uploadPart(request);
                            return result.getPartETag();
                          },
                          executorService)
                      .whenComplete(
                          (result, thrown) -> {
                            try {
                              Files.deleteIfExists(stagingFile.toPath());
                            } catch (IOException e) {
                              LOG.warn("Failed to delete staging file: {}", stagingFile, e);
                            }

                            if (thrown != null) {
                              LOG.error("Failed to upload part: {}", partNumber, thrown);
                            }
                          });

              parts.put(stagingFile, future);
            });
  }

  private void completeMultiPartUpload() {
    Preconditions.checkState(closed, "Complete upload called on open stream: %s", uri);

    List<PartETag> partETags;
    try {
      partETags =
          parts.values().stream()
              .map(CompletableFuture::join)
              .sorted(Comparator.comparing(PartETag::getPartNumber))
              .collect(Collectors.toList());
    } catch (CompletionException ce) {
      parts.values().forEach(c -> c.cancel(true));
      try {
        abortUpload();
      } catch (Exception e) {
        LOG.warn("Failed to abort multipart upload: {}", multipartUploadId, e);
      }
      throw ce;
    }

    client.completeMultipartUpload(
        new CompleteMultipartUploadRequest(uri.bucket(), uri.key(), multipartUploadId, partETags));
  }

  private void completeUploads() {
    if (multipartUploadId == null) {
      long contentLength = stagingFiles.stream().mapToLong(File::length).sum();
      if (contentLength == 0) {
        LOG.debug("Skipping empty upload to OSS");
        return;
      }

      LOG.debug("Uploading {} staged bytes to OSS via putObject", contentLength);
      ObjectMetadata metadata = new ObjectMetadata();
      metadata.setContentLength(contentLength);

      InputStream inputStream =
          stagingFiles.stream()
              .map(OSSOutputStream::uncheckedInputStream)
              .reduce(SequenceInputStream::new)
              .orElseGet(() -> new ByteArrayInputStream(new byte[0]));

      client.putObject(new PutObjectRequest(uri.bucket(), uri.key(), inputStream, metadata));
    } else {
      uploadParts();
      completeMultiPartUpload();
    }
  }

  private void abortUpload() {
    if (multipartUploadId != null) {
      try {
        client.abortMultipartUpload(
            new AbortMultipartUploadRequest(uri.bucket(), uri.key(), multipartUploadId));
      } finally {
        cleanUpStagingFiles();
      }
    }
  }

  private void cleanUpStagingFiles() {
    for (File file : stagingFiles) {
      try {
        Files.deleteIfExists(file.toPath());
      } catch (IOException e) {
        LOG.warn("Failed to delete staging file: {}", file, e);
      }
    }
  }

  private void createStagingDirectoryIfNotExists() throws IOException {
    if (!stagingDirectory.exists()) {
      LOG.info(
          "Staging directory does not exist, trying to create one: {}",
          stagingDirectory.getAbsolutePath());
      boolean created = stagingDirectory.mkdirs();
      if (created) {
        LOG.info("Successfully created staging directory: {}", stagingDirectory.getAbsolutePath());
      } else if (!stagingDirectory.exists()) {
        throw new IOException(
            "Failed to create staging directory: " + stagingDirectory.getAbsolutePath());
      }
    }
  }

  private static InputStream uncheckedInputStream(File file) {
    try {
      return new FileInputStream(file);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @SuppressWarnings({"checkstyle:NoFinalizer", "Finalize", "deprecation"})
  @Override
  protected void finalize() throws Throwable {
    super.finalize();
    if (!closed) {
      close(false); // releasing resources is more important than printing the warning
      String trace = Joiner.on("\n\t").join(Arrays.copyOfRange(createStack, 1, createStack.length));
      LOG.warn("Unclosed output stream created by:\n\t{}", trace);
    }
  }
}
