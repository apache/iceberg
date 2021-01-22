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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Predicates;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.io.CountingOutputStream;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.MoreExecutors;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

class S3OutputStream extends PositionOutputStream {
  private static final Logger LOG = LoggerFactory.getLogger(S3OutputStream.class);

  private static volatile ExecutorService executorService;

  private final StackTraceElement[] createStack;
  private final S3Client s3;
  private final S3URI location;
  private final AwsProperties awsProperties;

  private CountingOutputStream stream;
  private final List<File> stagingFiles = Lists.newArrayList();
  private final File stagingDirectory;
  private File currentStagingFile;
  private String multipartUploadId;
  private final Map<File, CompletableFuture<CompletedPart>> multiPartMap = Maps.newHashMap();
  private final int multiPartSize;
  private final int multiPartThresholdSize;

  private long pos = 0;
  private boolean closed = false;

  @SuppressWarnings("StaticAssignmentInConstructor")
  S3OutputStream(S3Client s3, S3URI location, AwsProperties awsProperties) throws IOException {
    if (executorService == null) {
      synchronized (S3OutputStream.class) {
        if (executorService == null) {
          executorService = MoreExecutors.getExitingExecutorService(
              (ThreadPoolExecutor) Executors.newFixedThreadPool(
                  awsProperties.s3FileIoMultipartUploadThreads(),
                  new ThreadFactoryBuilder()
                      .setDaemon(true)
                      .setNameFormat("iceberg-s3fileio-upload-%d")
                      .build()));
        }
      }
    }

    this.s3 = s3;
    this.location = location;
    this.awsProperties = awsProperties;

    createStack = Thread.currentThread().getStackTrace();

    multiPartSize = awsProperties.s3FileIoMultiPartSize();
    multiPartThresholdSize =  (int) (multiPartSize * awsProperties.s3FileIOMultipartThresholdFactor());
    stagingDirectory = new File(awsProperties.s3fileIoStagingDirectory());

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

    // switch to multipart upload
    if (multipartUploadId == null && pos >= multiPartThresholdSize) {
      initializeMultiPartUpload();
      uploadParts();
    }
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    int remaining = len;
    int relativeOffset = off;

    // Write the remainder of the part size to the staging file
    // and continue to write new staging files if the write is
    // larger than the part size.
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

    // switch to multipart upload
    if (multipartUploadId == null && pos >= multiPartThresholdSize) {
      initializeMultiPartUpload();
      uploadParts();
    }
  }

  private void newStream() throws IOException {
    if (stream != null) {
      stream.close();
    }

    currentStagingFile = File.createTempFile("s3fileio-", ".tmp", stagingDirectory);
    currentStagingFile.deleteOnExit();
    stagingFiles.add(currentStagingFile);

    stream = new CountingOutputStream(new BufferedOutputStream(new FileOutputStream(currentStagingFile)));
  }

  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }

    super.close();
    closed = true;

    try {
      stream.close();

      completeUploads();
    } finally {
      cleanUpStagingFiles();
    }
  }

  private void initializeMultiPartUpload() {
    CreateMultipartUploadRequest.Builder requestBuilder = CreateMultipartUploadRequest.builder()
        .bucket(location.bucket()).key(location.key());
    S3RequestUtil.configureEncryption(awsProperties, requestBuilder);
    S3RequestUtil.configurePermission(awsProperties, requestBuilder);

    multipartUploadId = s3.createMultipartUpload(requestBuilder.build()).uploadId();
  }

  private void uploadParts() {
    // exit if multipart has not been initiated
    if (multipartUploadId == null) {
      return;
    }

    stagingFiles.stream()
        // do not upload the file currently being written
        .filter(f -> closed || !f.equals(currentStagingFile))
        // do not upload any files that have already been processed
        .filter(Predicates.not(multiPartMap::containsKey))
        .forEach(f -> {
          UploadPartRequest.Builder requestBuilder = UploadPartRequest.builder()
              .bucket(location.bucket())
              .key(location.key())
              .uploadId(multipartUploadId)
              .partNumber(stagingFiles.indexOf(f) + 1)
              .contentLength(f.length());

          S3RequestUtil.configureEncryption(awsProperties, requestBuilder);

          UploadPartRequest uploadRequest = requestBuilder.build();

          CompletableFuture<CompletedPart> future = CompletableFuture.supplyAsync(
              () -> {
                UploadPartResponse response = s3.uploadPart(uploadRequest, RequestBody.fromFile(f));
                return CompletedPart.builder().eTag(response.eTag()).partNumber(uploadRequest.partNumber()).build();
              },
              executorService
          ).whenComplete((result, thrown) -> {
            try {
              Files.deleteIfExists(f.toPath());
            } catch (IOException e) {
              LOG.warn("Failed to delete staging file: {}", f, e);
            }

            if (thrown != null) {
              LOG.error("Failed to upload part: {}", uploadRequest, thrown);
              abortUpload();
            }
          });

          multiPartMap.put(f, future);
        });
  }

  private void completeMultiPartUpload() {
    Preconditions.checkState(closed, "Complete upload called on open stream: " + location);

    List<CompletedPart> completedParts =
        multiPartMap.values()
            .stream()
            .map(CompletableFuture::join)
            .sorted(Comparator.comparing(CompletedPart::partNumber))
            .collect(Collectors.toList());

    CompleteMultipartUploadRequest request = CompleteMultipartUploadRequest.builder()
        .bucket(location.bucket()).key(location.key())
        .uploadId(multipartUploadId)
        .multipartUpload(CompletedMultipartUpload.builder().parts(completedParts).build()).build();

    Tasks.foreach(request)
        .noRetry()
        .onFailure((r, thrown) -> {
          LOG.error("Failed to complete multipart upload request: {}", r, thrown);
          abortUpload();
        })
        .throwFailureWhenFinished()
        .run(s3::completeMultipartUpload);
  }

  private void abortUpload() {
    if (multipartUploadId != null) {
      try {
        s3.abortMultipartUpload(AbortMultipartUploadRequest.builder()
            .bucket(location.bucket()).key(location.key()).uploadId(multipartUploadId).build());
      } finally {
        cleanUpStagingFiles();
      }
    }
  }

  private void cleanUpStagingFiles() {
    Tasks.foreach(stagingFiles)
        .suppressFailureWhenFinished()
        .onFailure((file, thrown) -> LOG.warn("Failed to delete staging file: {}", file, thrown))
        .run(File::delete);
  }

  private void completeUploads() {
    if (multipartUploadId == null) {
      long contentLength = stagingFiles.stream().mapToLong(File::length).sum();
      InputStream contentStream = new BufferedInputStream(stagingFiles.stream()
          .map(S3OutputStream::uncheckedInputStream)
          .reduce(SequenceInputStream::new)
          .orElseGet(() -> new ByteArrayInputStream(new byte[0])));

      PutObjectRequest.Builder requestBuilder = PutObjectRequest.builder()
          .bucket(location.bucket())
          .key(location.key());

      S3RequestUtil.configureEncryption(awsProperties, requestBuilder);
      S3RequestUtil.configurePermission(awsProperties, requestBuilder);

      s3.putObject(requestBuilder.build(), RequestBody.fromInputStream(contentStream, contentLength));
    } else {
      uploadParts();
      completeMultiPartUpload();
    }
  }

  private static InputStream uncheckedInputStream(File file) {
    try {
      return new FileInputStream(file);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @SuppressWarnings("checkstyle:NoFinalizer")
  @Override
  protected void finalize() throws Throwable {
    super.finalize();
    if (!closed) {
      close(); // releasing resources is more important than printing the warning
      String trace = Joiner.on("\n\t").join(
          Arrays.copyOfRange(createStack, 1, createStack.length));
      LOG.warn("Unclosed output stream created by:\n\t{}", trace);
    }
  }
}
