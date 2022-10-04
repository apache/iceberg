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
package org.apache.iceberg.gcp.gcs;

import com.google.api.client.util.Lists;
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobSourceOption;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.io.FileIOMetricsContext;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.metrics.Counter;
import org.apache.iceberg.metrics.MetricsContext;
import org.apache.iceberg.metrics.MetricsContext.Unit;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The GCSInputStream leverages native streaming channels from the GCS API for streaming uploads.
 * See <a href="https://cloud.google.com/storage/docs/streaming">Streaming Transfers</a>
 */
class GCSInputStream extends SeekableInputStream {
  private static final Logger LOG = LoggerFactory.getLogger(GCSInputStream.class);

  private final StackTraceElement[] createStack;
  private final Storage storage;
  private final BlobId blobId;
  private final GCPProperties gcpProperties;

  private ReadChannel channel;
  private long pos = 0;
  private boolean closed = false;
  private final ByteBuffer singleByteBuffer = ByteBuffer.wrap(new byte[1]);
  private ByteBuffer byteBuffer;

  private final Counter readBytes;
  private final Counter readOperations;

  GCSInputStream(
      Storage storage, BlobId blobId, GCPProperties gcpProperties, MetricsContext metrics) {
    this.storage = storage;
    this.blobId = blobId;
    this.gcpProperties = gcpProperties;

    this.readBytes = metrics.counter(FileIOMetricsContext.READ_BYTES, Unit.BYTES);
    this.readOperations = metrics.counter(FileIOMetricsContext.READ_OPERATIONS);

    createStack = Thread.currentThread().getStackTrace();

    openStream();
  }

  private void openStream() {
    List<BlobSourceOption> sourceOptions = Lists.newArrayList();

    gcpProperties
        .decryptionKey()
        .ifPresent(key -> sourceOptions.add(BlobSourceOption.decryptionKey(key)));
    gcpProperties
        .userProject()
        .ifPresent(userProject -> sourceOptions.add(BlobSourceOption.userProject(userProject)));

    channel = storage.reader(blobId, sourceOptions.toArray(new BlobSourceOption[0]));

    gcpProperties.channelReadChunkSize().ifPresent(channel::setChunkSize);
  }

  @Override
  public long getPos() {
    return pos;
  }

  @Override
  public void seek(long newPos) {
    Preconditions.checkState(!closed, "already closed");
    Preconditions.checkArgument(newPos >= 0, "position is negative: %s", newPos);

    pos = newPos;
    try {
      channel.seek(newPos);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public int read() throws IOException {
    Preconditions.checkState(!closed, "Cannot read: already closed");
    singleByteBuffer.position(0);

    pos += 1;
    channel.read(singleByteBuffer);
    readBytes.increment();
    readOperations.increment();

    return singleByteBuffer.array()[0];
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    Preconditions.checkState(!closed, "Cannot read: already closed");

    byteBuffer = byteBuffer != null && byteBuffer.array() == b ? byteBuffer : ByteBuffer.wrap(b);
    byteBuffer.position(off);
    byteBuffer.limit(Math.min(off + len, byteBuffer.capacity()));

    int bytesRead = channel.read(byteBuffer);
    pos += bytesRead;
    readBytes.increment(bytesRead);
    readOperations.increment();

    return bytesRead;
  }

  @Override
  public void close() throws IOException {
    super.close();
    closed = true;
    if (channel != null) {
      channel.close();
    }
  }

  @SuppressWarnings("checkstyle:NoFinalizer")
  @Override
  protected void finalize() throws Throwable {
    super.finalize();
    if (!closed) {
      close(); // releasing resources is more important than printing the warning
      String trace = Joiner.on("\n\t").join(Arrays.copyOfRange(createStack, 1, createStack.length));
      LOG.warn("Unclosed input stream created by:\n\t{}", trace);
    }
  }
}
