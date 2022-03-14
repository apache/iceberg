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

package org.apache.iceberg.azure.blob;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.models.BlobRange;
import com.azure.storage.blob.models.BlobRequestConditions;
import com.azure.storage.blob.specialized.BlobInputStream;
import java.io.IOException;
import java.util.Arrays;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AzureBlobInputStream extends SeekableInputStream {

  private static final Logger LOG = LoggerFactory.getLogger(AzureBlobInputStream.class);
  private static final Joiner TRACE_JOINER = Joiner.on("\n\t");

  private final StackTraceElement[] createStack;
  private final BlobClient blobClient;

  private long pos = 0L;
  private boolean closed = false;
  private BlobInputStream stream;

  public AzureBlobInputStream(BlobClient blobClient) {
    this.createStack = Thread.currentThread().getStackTrace();
    this.blobClient = blobClient;
    openStream(/* offset = */ 0);
  }

  @Override
  public long getPos() {
    return pos;
  }

  @Override
  public void seek(long newPos) {
    Preconditions.checkState(!closed, "Stream already closed");
    Preconditions.checkArgument(newPos >= 0, "New position cannot be negative: %s", newPos);

    if (newPos == pos) {
      // Already at the specified position.
      return;
    }

    if (newPos > pos) {
      // Seeking forward.
      final long bytesToSkip = newPos - pos;
      // BlobInputStream#skip only repositions the internal pointers,
      // the actual bytes are skipped when BlobInputStream#read is invoked.
      final long bytesSkipped = stream.skip(bytesToSkip);
    } else {
      // Seeking backward.
      stream.close();
      openStream(newPos);
    }

    pos = newPos;
  }

  @Override
  public int read() throws IOException {
    Preconditions.checkState(!closed, "Cannot read: stream already closed");
    pos++;
    return stream.read();
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    Preconditions.checkState(!closed, "Cannot read: stream already closed");
    int numOfBytesRead = stream.read(b, off, len);
    pos += numOfBytesRead;
    return numOfBytesRead;
  }

  @Override
  public void close() throws IOException {
    closed = true;
    super.close();
    stream.close();
  }

  @SuppressWarnings("checkstyle:NoFinalizer")
  @Override
  protected void finalize() throws Throwable {
    super.finalize();
    if (!closed) {
      close(); // releasing resources is more important than printing the warning
      String trace = TRACE_JOINER.join(Arrays.copyOfRange(createStack, 1, createStack.length));
      LOG.warn("Unclosed input stream created by:\n\t{}", trace);
    }
  }

  private void openStream(long offset) {
    stream = blobClient.openInputStream(new BlobRange(offset), new BlobRequestConditions());
    pos = offset;
  }
}
