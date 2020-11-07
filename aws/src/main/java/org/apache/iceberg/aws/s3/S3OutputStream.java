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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

class S3OutputStream extends PositionOutputStream {
  private static final Logger LOG = LoggerFactory.getLogger(S3OutputStream.class);

  private final StackTraceElement[] createStack;
  private final S3Client s3;
  private final S3URI location;

  private final OutputStream stream;
  private final File stagingFile;
  private long pos = 0;

  private boolean closed = false;

  S3OutputStream(S3Client s3, S3URI location) throws IOException {
    this.s3 = s3;
    this.location = location;

    createStack = Thread.currentThread().getStackTrace();
    stagingFile = File.createTempFile("s3fileio-", ".tmp");
    stream = new BufferedOutputStream(new FileOutputStream(stagingFile));

    stagingFile.deleteOnExit();
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
    stream.write(b);
    pos += 1;
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    stream.write(b, off, len);
    pos += len;
  }

  @Override
  public void close() throws IOException {
    super.close();
    closed = true;

    try {
      stream.close();

      s3.putObject(
          PutObjectRequest.builder().bucket(location.bucket()).key(location.key()).build(),
          RequestBody.fromFile(stagingFile));
    } finally {
      if (!stagingFile.delete()) {
        LOG.warn("Could not delete temporary file: {}", stagingFile);
      }
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
