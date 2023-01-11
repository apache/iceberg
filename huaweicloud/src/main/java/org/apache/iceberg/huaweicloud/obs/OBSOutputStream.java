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
package org.apache.iceberg.huaweicloud.obs;

import com.obs.services.IObsClient;
import com.obs.services.model.ObjectMetadata;
import com.obs.services.model.PutObjectRequest;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.Arrays;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.huaweicloud.HuaweicloudProperties;
import org.apache.iceberg.io.FileIOMetricsContext;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.metrics.MetricsContext;
import org.apache.iceberg.metrics.MetricsContext.Counter;
import org.apache.iceberg.metrics.MetricsContext.Unit;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class OBSOutputStream extends PositionOutputStream {
  private static final Logger LOG = LoggerFactory.getLogger(OBSOutputStream.class);
  private final StackTraceElement[] createStack;

  private final IObsClient client;
  private final OBSURI uri;

  private final File currentStagingFile;
  private final OutputStream stream;
  private long pos = 0;
  private boolean closed = false;

  private final Counter<Long> writeBytes;
  private final Counter<Integer> writeOperations;

  OBSOutputStream(
      IObsClient client,
      OBSURI uri,
      HuaweicloudProperties huaweicloudProperties,
      MetricsContext metrics) {
    this.client = client;
    this.uri = uri;
    this.createStack = Thread.currentThread().getStackTrace();

    this.currentStagingFile = newStagingFile(huaweicloudProperties.obsStagingDirectory());
    this.stream = newStream(currentStagingFile);
    this.writeBytes = metrics.counter(FileIOMetricsContext.WRITE_BYTES, Long.class, Unit.BYTES);
    this.writeOperations =
        metrics.counter(FileIOMetricsContext.WRITE_OPERATIONS, Integer.class, Unit.COUNT);
  }

  private static File newStagingFile(String obsStagingDirectory) {
    try {
      File stagingFile = File.createTempFile("obs-file-io-", ".tmp", new File(obsStagingDirectory));
      stagingFile.deleteOnExit();
      return stagingFile;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static OutputStream newStream(File currentStagingFile) {
    try {
      return new BufferedOutputStream(new FileOutputStream(currentStagingFile));
    } catch (FileNotFoundException e) {
      throw new NotFoundException(e, "Failed to create file: %s", currentStagingFile);
    }
  }

  private static InputStream uncheckedInputStream(File file) {
    try {
      return Files.newInputStream(file.toPath());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public long getPos() {
    return pos;
  }

  @Override
  public void flush() throws IOException {
    Preconditions.checkState(!closed, "Already closed.");
    stream.flush();
  }

  @Override
  public void write(int b) throws IOException {
    Preconditions.checkState(!closed, "Already closed.");
    stream.write(b);
    pos += 1;
    writeBytes.increment();
    writeOperations.increment();
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    Preconditions.checkState(!closed, "Already closed.");
    stream.write(b, off, len);
    pos += len;
    writeBytes.increment((long) len);
    writeOperations.increment();
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

  private void completeUploads() {
    long contentLength = currentStagingFile.length();
    if (contentLength == 0) {
      LOG.debug("Skipping empty upload to OBS");
      return;
    }

    LOG.debug("Uploading {} staged bytes to OBS", contentLength);
    InputStream contentStream = uncheckedInputStream(currentStagingFile);
    ObjectMetadata metadata = new ObjectMetadata();
    metadata.setContentLength(contentLength);

    PutObjectRequest request = new PutObjectRequest(uri.bucket(), uri.key(), contentStream);
    request.setMetadata(metadata);
    client.putObject(request);
  }

  private void cleanUpStagingFiles() {
    if (!currentStagingFile.delete()) {
      LOG.warn("Failed to delete staging file: {}", currentStagingFile);
    }
  }

  @SuppressWarnings("checkstyle:NoFinalizer")
  @Override
  protected void finalize() throws Throwable {
    super.finalize();
    if (!closed) {
      close(); // releasing resources is more important than printing the warning.
      String trace = Joiner.on("\n\t").join(Arrays.copyOfRange(createStack, 1, createStack.length));
      LOG.warn("Unclosed output stream created by:\n\t{}", trace);
    }
  }
}
