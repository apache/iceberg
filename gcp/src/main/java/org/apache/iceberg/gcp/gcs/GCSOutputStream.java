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
 * <ul>
 *   <li>If size &lt; {@link GCPProperties#GCS_WRITE_THRESHOLD_BYTES} (default 8MB): single-shot
 *       {@link Storage#create}
 *   <li>Otherwise: {@link WriteChannel} output stream
 * </ul>
 */
class GCSOutputStream extends PositionOutputStream {
  private static final Logger LOG = LoggerFactory.getLogger(GCSOutputStream.class);

  private final StackTraceElement[] createStack;
  private final Storage storage;
  private final BlobId blobId;
  private final GCPProperties gcpProperties;
  private final long writeThreshold;

  private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
  private OutputStream stream;
  private boolean useWriteChannel = false;

  private final Counter writeBytes;
  private final Counter writeOperations;

  private long pos = 0;
  private boolean closed = false;

  GCSOutputStream(
      Storage storage, BlobId blobId, GCPProperties gcpProperties, MetricsContext metrics) {
    this.storage = storage;
    this.blobId = blobId;
    this.gcpProperties = gcpProperties;
    this.writeThreshold = gcpProperties.writeThresholdBytes();

    createStack = Thread.currentThread().getStackTrace();

    this.writeBytes = metrics.counter(FileIOMetricsContext.WRITE_BYTES, Unit.BYTES);
    this.writeOperations = metrics.counter(FileIOMetricsContext.WRITE_OPERATIONS);

    // Buffer in memory until size reaches the threshold.
    this.stream = buffer;
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
    writeBytes.increment();
    writeOperations.increment();

    if (!useWriteChannel && pos >= writeThreshold) {
      openStream();
    }
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    stream.write(b, off, len);
    pos += len;
    writeBytes.increment(len);
    writeOperations.increment();

    if (!useWriteChannel && pos >= writeThreshold) {
      openStream();
    }
  }

  /** Switch from in-memory buffer to a WriteChannel once size >= threshold. */
  private void openStream() throws IOException {
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

    OutputStream channelStream = Channels.newOutputStream(channel);
    buffer.writeTo(channelStream);
    buffer.reset();
    stream = channelStream;
    useWriteChannel = true;
  }

  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }

    super.close();
    closed = true;

    if (useWriteChannel) {
      stream.close();
      return;
    }

    // size < threshold → single-shot upload
    List<BlobTargetOption> targetOptions = Lists.newArrayList();
    gcpProperties
        .encryptionKey()
        .ifPresent(key -> targetOptions.add(BlobTargetOption.encryptionKey(key)));
    gcpProperties
        .userProject()
        .ifPresent(userProject -> targetOptions.add(BlobTargetOption.userProject(userProject)));

    storage.create(
        BlobInfo.newBuilder(blobId).build(),
        buffer.toByteArray(),
        targetOptions.toArray(new BlobTargetOption[0]));
  }

  @SuppressWarnings({"checkstyle:NoFinalizer", "Finalize", "deprecation"})
  @Override
  protected void finalize() throws Throwable {
    super.finalize();
    if (!closed) {
      close();
      String trace = Joiner.on("\n\t").join(Arrays.copyOfRange(createStack, 1, createStack.length));
      LOG.warn("Unclosed output stream created by:\n\t{}", trace);
    }
  }
}
