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

import com.google.cloud.WriteChannel;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobTargetOption;
import com.google.cloud.storage.Storage.BlobWriteOption;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.io.FileIOMetricsContext;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.metrics.Counter;
import org.apache.iceberg.metrics.MetricsContext;
import org.apache.iceberg.metrics.MetricsContext.Unit;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Uploads to GCS.
 *
 * <p>Objects smaller than {@link GCPProperties#GCS_WRITE_THRESHOLD_BYTES} are uploaded with a
 * single {@link Storage#create} call on close. Larger objects use a {@link WriteChannel} and
 * stream, which is the pre-existing GCS write path.
 */
class GCSOutputStream extends PositionOutputStream {
  private static final Logger LOG = LoggerFactory.getLogger(GCSOutputStream.class);

  private final StackTraceElement[] createStack;
  private final Storage storage;
  private final BlobId blobId;
  private final GCPProperties gcpProperties;
  private final int writeThreshold;

  private ByteArrayOutputStream buffer;
  private OutputStream stream;
  private boolean useWriteChannel = false;

  private final Counter writeBytes;
  private final Counter writeOperations;

  private long pos = 0;
  private boolean closed = false;

  GCSOutputStream(
      Storage storage, BlobId blobId, GCPProperties gcpProperties, MetricsContext metrics)
      throws IOException {
    this.storage = storage;
    this.blobId = blobId;
    this.gcpProperties = gcpProperties;
    this.writeThreshold = (int) gcpProperties.writeThresholdBytes();

    createStack = Thread.currentThread().getStackTrace();

    this.writeBytes = metrics.counter(FileIOMetricsContext.WRITE_BYTES, Unit.BYTES);
    this.writeOperations = metrics.counter(FileIOMetricsContext.WRITE_OPERATIONS);

    if (writeThreshold == 0) {
      openWriteChannel();
    } else {
      this.buffer = new ByteArrayOutputStream();
      this.stream = buffer;
    }
  }

  @Override
  public long getPos() {
    return pos;
  }

  @Override
  public void flush() throws IOException {
    if (stream != null) {
      stream.flush();
    }
  }

  @Override
  public void write(int b) throws IOException {
    stream.write(b);
    pos += 1;
    writeBytes.increment();
    writeOperations.increment();

    if (!useWriteChannel && pos >= writeThreshold) {
      switchToWriteChannel();
    }
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    int remaining = len;
    int offset = off;

    if (!useWriteChannel && pos + remaining >= writeThreshold) {
      int toThreshold = writeThreshold - (int) pos;
      if (toThreshold > 0) {
        stream.write(b, offset, toThreshold);
        pos += toThreshold;
        offset += toThreshold;
        remaining -= toThreshold;
      }
      switchToWriteChannel();
    }

    if (remaining > 0) {
      stream.write(b, offset, remaining);
      pos += remaining;
    }

    writeBytes.increment(len);
    writeOperations.increment();
  }

  /**
   * Open the existing WriteChannel streaming path. Once {@link Storage#writer} succeeds, close()
   * must not fall through to {@link Storage#create}.
   */
  private void switchToWriteChannel() throws IOException {
    OutputStream channelStream = openWriteChannel();
    try {
      buffer.writeTo(channelStream);
      buffer = null;
    } catch (IOException e) {
      try {
        // Best-effort abort. A failed close can leave a truncated GCS object; useWriteChannel
        // stays true so close() does not also Storage.create the same path.
        channelStream.close();
      } catch (IOException closeException) {
        e.addSuppressed(closeException);
      }
      stream = null;
      throw e;
    }
  }

  private OutputStream openWriteChannel() {
    OutputStream channelStream = newWriteChannelStream();
    this.stream = channelStream;
    this.useWriteChannel = true;
    return channelStream;
  }

  private OutputStream newWriteChannelStream() {
    List<BlobWriteOption> writeOptions = Lists.newArrayList();

    gcpProperties
        .encryptionKey()
        .ifPresent(key -> writeOptions.add(BlobWriteOption.encryptionKey(key)));
    gcpProperties
        .userProject()
        .ifPresent(userProject -> writeOptions.add(BlobWriteOption.userProject(userProject)));

    WriteChannel channel =
        storage.writer(
            BlobInfo.newBuilder(blobId).build(), writeOptions.toArray(new BlobWriteOption[0]));

    gcpProperties.channelWriteChunkSize().ifPresent(channel::setChunkSize);

    return Channels.newOutputStream(channel);
  }

  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }

    super.close();
    closed = true;

    if (useWriteChannel) {
      if (stream != null) {
        stream.close();
      }
      return;
    }

    // size < threshold: single-shot upload
    List<BlobTargetOption> targetOptions = Lists.newArrayList();
    gcpProperties
        .encryptionKey()
        .ifPresent(key -> targetOptions.add(BlobTargetOption.encryptionKey(key)));
    gcpProperties
        .userProject()
        .ifPresent(userProject -> targetOptions.add(BlobTargetOption.userProject(userProject)));

    byte[] content = buffer != null ? buffer.toByteArray() : new byte[0];
    buffer = null;
    storage.create(
        BlobInfo.newBuilder(blobId).build(),
        content,
        targetOptions.toArray(new BlobTargetOption[0]));
  }

  @SuppressWarnings({"checkstyle:NoFinalizer", "Finalize", "deprecation"})
  @Override
  protected void finalize() throws Throwable {
    super.finalize();
    if (!closed) {
      close(); // releasing resources is more important than printing the warning
      String trace = Joiner.on("\n\t").join(Arrays.copyOfRange(createStack, 1, createStack.length));
      LOG.warn("Unclosed output stream created by:\n\t{}", trace);
    }
  }
}
