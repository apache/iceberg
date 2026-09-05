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
import com.google.cloud.storage.Storage.BlobWriteOption;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
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
 * The GCSOutputStream leverages native streaming channels from the GCS API for streaming uploads.
 * See <a href="https://cloud.google.com/storage/docs/streaming">Streaming Transfers</a>
 */
class GCSOutputStream extends PositionOutputStream {
  private static final Logger LOG = LoggerFactory.getLogger(GCSOutputStream.class);

  private final StackTraceElement[] createStack;
  private final Storage storage;
  private final BlobInfo blobInfo;
  private final GCPProperties gcpProperties;

  private OutputStream stream;

  private final Counter writeBytes;
  private final Counter writeOperations;

  private long pos = 0;
  private boolean closed = false;

  GCSOutputStream(
      Storage storage, BlobId blobId, GCPProperties gcpProperties, MetricsContext metrics)
      throws IOException {
    this.storage = storage;
    this.gcpProperties = gcpProperties;
    this.blobInfo = buildBlobInfo(blobId, gcpProperties);

    createStack = Thread.currentThread().getStackTrace();

    this.writeBytes = metrics.counter(FileIOMetricsContext.WRITE_BYTES, Unit.BYTES);
    this.writeOperations = metrics.counter(FileIOMetricsContext.WRITE_OPERATIONS);

    openStream();
  }

  /**
   * Builds the {@link BlobInfo} used to initiate the resumable upload.
   *
   * <p>When {@code gcs.write.object-context.*} properties are present, builds an {@link
   * BlobInfo.ObjectContexts} from them and sets it on the builder via {@code
   * BlobInfo.Builder.setContexts()}. The SDK then includes the contexts in the {@code
   * WriteObjectRequest} / {@code StartResumableWriteRequest} wire payload.
   *
   * <p>When no context properties are configured this method behaves identically to the original
   * implementation — {@code setContexts} is never called and there is zero overhead.
   */
  private static BlobInfo buildBlobInfo(BlobId blobId, GCPProperties gcpProperties) {
    BlobInfo.Builder builder = BlobInfo.newBuilder(blobId);

    Map<String, String> writeContexts = gcpProperties.writeObjectContexts();
    if (writeContexts != null && !writeContexts.isEmpty()) {
      Map<String, BlobInfo.ObjectCustomContextPayload> payloads =
          writeContexts.entrySet().stream()
              .collect(
                  Collectors.toMap(
                      Map.Entry::getKey,
                      e ->
                          BlobInfo.ObjectCustomContextPayload.newBuilder()
                              .setValue(e.getValue())
                              .build()));

      builder.setContexts(BlobInfo.ObjectContexts.newBuilder().setCustom(payloads).build());
    }

    return builder.build();
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
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    stream.write(b, off, len);
    pos += len;
    writeBytes.increment(len);
    writeOperations.increment();
  }

  private void openStream() {
    List<BlobWriteOption> writeOptions = Lists.newArrayList();

    gcpProperties
        .encryptionKey()
        .ifPresent(key -> writeOptions.add(BlobWriteOption.encryptionKey(key)));
    gcpProperties
        .userProject()
        .ifPresent(userProject -> writeOptions.add(BlobWriteOption.userProject(userProject)));

    WriteChannel channel = storage.writer(blobInfo, writeOptions.toArray(new BlobWriteOption[0]));

    gcpProperties.channelWriteChunkSize().ifPresent(channel::setChunkSize);

    stream = Channels.newOutputStream(channel);
  }

  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }

    super.close();
    closed = true;
    stream.close();
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
