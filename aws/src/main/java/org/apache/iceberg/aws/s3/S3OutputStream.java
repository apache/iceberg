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
import java.util.Arrays;
import java.util.Locale;
import org.apache.iceberg.aws.AwsProperties;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Predicates;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.io.CountingOutputStream;
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
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

class S3OutputStream extends PositionOutputStream {
  private static final Logger LOG = LoggerFactory.getLogger(S3OutputStream.class);

  static final String MULTIPART_SIZE = "s3fileio.multipart.size";
  static final String MULTIPART_THRESHOLD_FACTOR = "s3fileio.multipart.threshold";

  static final int MIN_MULTIPART_UPLOAD_SIZE = 5 * 1024 * 1024;
  static final int DEFAULT_MULTIPART_SIZE = 32 * 1024 * 1024;
  static final double DEFAULT_MULTIPART_THRESHOLD = 1.5;

  private final StackTraceElement[] createStack;
  private final S3Client s3;
  private final S3URI location;
  private final AwsProperties awsProperties;

  private CountingOutputStream stream;
  private final List<File> stagingFiles = Lists.newArrayList();
  private File currentStagingFile;
  private String multipartUploadId;
  private final Map<File, CompletableFuture<CompletedPart>> multiPartMap = Maps.newHashMap();
  private final int multiPartSize;
  private final double multiPartThresholdFactor;

  private long pos = 0;
  private boolean closed = false;

  S3OutputStream(S3Client s3, S3URI location) throws IOException {
    this(s3, location, new AwsProperties());
  }

  S3OutputStream(S3Client s3, S3URI location, AwsProperties awsProperties) throws IOException {
    this.s3 = s3;
    this.location = location;
    this.awsProperties = awsProperties;

    createStack = Thread.currentThread().getStackTrace();

    multiPartSize = Integer.parseInt(properties.getOrDefault(MULTIPART_SIZE, Integer.toString(DEFAULT_MULTIPART_SIZE)));
    multiPartThresholdFactor = Double.parseDouble(properties.getOrDefault(
        MULTIPART_THRESHOLD_FACTOR,
        Double.toString(DEFAULT_MULTIPART_THRESHOLD)));

    Preconditions.checkArgument(multiPartSize * multiPartThresholdFactor > MIN_MULTIPART_UPLOAD_SIZE,
        "Minimum multipart upload object size must be larger than 5 MB.");

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
    if (pos >= multiPartSize * multiPartThresholdFactor) {
      initializeMultiPartUpload();
      uploadParts();
    }
  }

  private void newStream() throws IOException {
    if (stream != null) {
      stream.close();
    }

    currentStagingFile = File.createTempFile("s3fileio-", ".tmp");
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
    currentStagingFile = null;

    try {
      stream.close();

      completeUploads();
    } finally {
      stagingFiles.forEach(f -> {
        if (f.exists() && !f.delete()) {
          LOG.warn("Could not delete temporary file: {}", f);
        }
      });
    }
  }

  private void initializeMultiPartUpload() {
    multipartUploadId = s3.createMultipartUpload(CreateMultipartUploadRequest.builder()
        .bucket(location.bucket()).key(location.key()).build()).uploadId();
  }

  private void uploadParts() {
    // exit if multipart has not been initiated
    if (multipartUploadId == null) {
      return;
    }

    stagingFiles.stream()
        // do not upload the file currently being written
        .filter(f -> currentStagingFile == null || !currentStagingFile.equals(f))
        // do not upload any files that have already been processed
        .filter(Predicates.not(multiPartMap::containsKey))
        .forEach(f -> {
          UploadPartRequest uploadRequest = UploadPartRequest.builder()
              .bucket(location.bucket())
              .key(location.key())
              .uploadId(multipartUploadId)
              .partNumber(stagingFiles.indexOf(f) + 1)
              .contentLength(f.length())
              .build();

          CompletableFuture<CompletedPart> future = CompletableFuture.supplyAsync(
              () -> {
                UploadPartResponse response = s3.uploadPart(uploadRequest, RequestBody.fromFile(f));
                return CompletedPart.builder().eTag(response.eTag()).partNumber(uploadRequest.partNumber()).build();
              }
          );

          multiPartMap.put(f, future);
        });
  }

  private void completeMultiPartUpload() throws IOException {
    try {
      List<CompletedPart> completedParts =
          multiPartMap.values()
              .stream()
              .map(CompletableFuture::join)
              .sorted(Comparator.comparing(CompletedPart::partNumber))
              .collect(Collectors.toList());

      s3.completeMultipartUpload(CompleteMultipartUploadRequest.builder()
          .bucket(location.bucket()).key(location.key())
          .uploadId(multipartUploadId)
          .multipartUpload(CompletedMultipartUpload.builder().parts(completedParts).build()).build());
    } catch (CompletionException e) {
      abortUpload();
      throw new IOException("Multipart upload failed for upload id: " + multipartUploadId, e);
    }
  }

  private void abortUpload() {
    if (multipartUploadId != null) {
      s3.abortMultipartUpload(AbortMultipartUploadRequest.builder()
          .bucket(location.bucket()).key(location.key()).uploadId(multipartUploadId).build());
    }
  }

  private void completeUploads() throws IOException {
    if (multipartUploadId == null) {
      long contentLength = stagingFiles.stream().mapToLong(File::length).sum();
      InputStream contentStream = new BufferedInputStream(stagingFiles.stream()
          .map(S3OutputStream::uncheckedInputStream)
          .reduce(SequenceInputStream::new).orElseGet(() -> new ByteArrayInputStream(new byte[0])));

      PutObjectRequest.Builder requestBuilder = PutObjectRequest.builder()
          .bucket(location.bucket())
          .key(location.key());

      switch (awsProperties.s3FileIoSseType().toLowerCase(Locale.ENGLISH)) {
        case AwsProperties.S3FILEIO_SSE_TYPE_NONE:
          break;

        case AwsProperties.S3FILEIO_SSE_TYPE_KMS:
          requestBuilder.serverSideEncryption(ServerSideEncryption.AWS_KMS);
          requestBuilder.ssekmsKeyId(awsProperties.s3FileIoSseKey());
          break;

        case AwsProperties.S3FILEIO_SSE_TYPE_S3:
          requestBuilder.serverSideEncryption(ServerSideEncryption.AES256);
          break;

        case AwsProperties.S3FILEIO_SSE_TYPE_CUSTOM:
          requestBuilder.sseCustomerAlgorithm(ServerSideEncryption.AES256.name());
          requestBuilder.sseCustomerKey(awsProperties.s3FileIoSseKey());
          requestBuilder.sseCustomerKeyMD5(awsProperties.s3FileIoSseMd5());
          break;

        default:
          throw new IllegalArgumentException(
              "Cannot support given S3 encryption type: " + awsProperties.s3FileIoSseType());
      }

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
