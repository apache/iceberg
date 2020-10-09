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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import java.io.IOException;
import java.util.Arrays;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3InputStream extends SeekableInputStream {
  private static final Logger LOG = LoggerFactory.getLogger(S3InputStream.class);

  private final StackTraceElement[] createStack;
  private final AmazonS3 s3;
  private final AmazonS3URI location;

  private S3ObjectInputStream stream;
  private long pos = 0;
  private long next = 0;
  private boolean closed = false;

  private int skipSize = 1024 * 1024;

  public S3InputStream(AmazonS3 s3, AmazonS3URI location) {
    this.s3 = s3;
    this.location = location;

    createStack = Thread.currentThread().getStackTrace();
  }

  @Override
  public long getPos() {
    return pos;
  }

  @Override
  public void seek(long newPos) {
    Preconditions.checkState(!closed, "already closed");
    Preconditions.checkArgument(newPos >= 0, "position is negative: %s", newPos);

    // this allows a seek beyond the end of the stream but the next read will fail
    next = newPos;
  }

  @Override
  public int read() {
    // This stream is wrapped with BufferedInputStream, so this method should never be called
    throw new UnsupportedOperationException();
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    Preconditions.checkState(!closed, "already closed");
    positionStream();

    int bytesRead = stream.read(b, off, len);
    pos += bytesRead;
    next += bytesRead;

    return bytesRead;
  }

  @Override
  public void close() throws IOException {
    super.close();
    closed = true;
  }

  private void positionStream() throws IOException {
    if ((stream != null) && (next == pos)) {
      // already at specified position
      return;
    }

    if ((stream != null) && (next > pos)) {
      // seeking forwards
      long skip = next - pos;
      if (skip <= Math.max(stream.available(), skipSize)) {
        // already buffered or seek is small enough
        LOG.debug("Read-through seek for {} to offset {}", location, next);
        IOUtils.skip(stream, skip);
        pos = next;
        return;
      }
    }

    // close the stream and open at desired position
    LOG.debug("Seek with new stream for {} to offset {}", location, next);
    pos = next;
    closeStream();
    openStream();
  }

  private void openStream() {
    GetObjectRequest request = new GetObjectRequest(location.getBucket(), location.getKey())
        .withRange(pos, Long.MAX_VALUE);
    stream = s3.getObject(request).getObjectContent();
  }

  private void closeStream() {
    if (stream != null) {
      stream.abort();
    }
  }

  public void setSkipSize(int skipSize) {
    this.skipSize = skipSize;
  }

  @SuppressWarnings("checkstyle:NoFinalizer")
  @Override
  protected void finalize() throws Throwable {
    super.finalize();
    if (!closed) {
      close(); // releasing resources is more important than printing the warning
      String trace = Joiner.on("\n\t").join(
          Arrays.copyOfRange(createStack, 1, createStack.length));
      LOG.warn("Unclosed input stream created by:\n\t{}", trace);
    }
  }
}
