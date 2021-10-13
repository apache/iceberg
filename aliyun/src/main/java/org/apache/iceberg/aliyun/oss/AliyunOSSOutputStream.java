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
import com.aliyun.oss.model.ObjectMetadata;
import com.aliyun.oss.model.PutObjectRequest;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Arrays;
import org.apache.iceberg.aliyun.AliyunProperties;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AliyunOSSOutputStream extends PositionOutputStream {
  private static final Logger LOG = LoggerFactory.getLogger(AliyunOSSOutputStream.class);
  private final StackTraceElement[] createStack;

  private final OSS client;
  private final AliyunOSSURI uri;
  private final AliyunProperties aliyunProperties;

  private File currentStagingFile;
  private BufferedOutputStream stream;
  private long pos = 0;
  private boolean closed = false;

  AliyunOSSOutputStream(OSS client, AliyunOSSURI uri, AliyunProperties aliyunProperties) throws IOException {
    this.client = client;
    this.uri = uri;
    this.aliyunProperties = aliyunProperties;
    this.createStack = Thread.currentThread().getStackTrace();

    newStream();
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
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    Preconditions.checkState(!closed, "Already closed.");
    stream.write(b, off, len);
    pos += len;
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

  private void newStream() throws IOException {
    if (stream != null) {
      stream.close();
    }
    Preconditions.checkArgument(aliyunProperties.ossStagingDirectory() != null, "stagingDirectory is null");
    File stagingDirectory = new File(aliyunProperties.ossStagingDirectory());
    currentStagingFile = File.createTempFile("oss-file-io-", ".tmp", stagingDirectory);
    currentStagingFile.deleteOnExit();

    stream = new BufferedOutputStream(new FileOutputStream(currentStagingFile));
  }

  private void completeUploads() {
    long contentLength = currentStagingFile.length();
    LOG.debug("Uploading current staging files to oss, total byte size is: {}", contentLength);
    if (contentLength == 0) {
      LOG.warn("invalid staging files to oss", contentLength);
      return;
    }
    InputStream contentStream = uncheckedInputStream(currentStagingFile);
    ObjectMetadata metadata = new ObjectMetadata();
    metadata.setContentLength(contentLength);

    PutObjectRequest request = new PutObjectRequest(uri.getBucket(), uri.getKey(), contentStream, metadata);
    client.putObject(request);
  }

  private static InputStream uncheckedInputStream(File file) {
    try {
      return new FileInputStream(file);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private void cleanUpStagingFiles() {
    if (!currentStagingFile.delete()) {
      LOG.warn("Failed to delete staging file: {}", currentStagingFile);
    }
    LOG.debug("Delete current staging file: {}", currentStagingFile);
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
