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
package org.apache.iceberg.dell.ecs;

import com.emc.object.Range;
import com.emc.object.s3.S3Client;
import java.io.IOException;
import java.io.InputStream;
import org.apache.iceberg.io.FileIOMetricsContext;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.metrics.Counter;
import org.apache.iceberg.metrics.MetricsContext;
import org.apache.iceberg.metrics.MetricsContext.Unit;

/**
 * A {@link SeekableInputStream} implementation that warp {@link S3Client#readObjectStream(String,
 * String, Range)}
 *
 * <ol>
 *   <li>The stream is only be loaded when start reading.
 *   <li>This class won't cache any bytes of content. It only maintains pos of {@link
 *       SeekableInputStream}
 *   <li>This class is not thread-safe.
 * </ol>
 */
class EcsSeekableInputStream extends SeekableInputStream {

  private final S3Client client;
  private final EcsURI uri;

  /** Mutable pos set by {@link #seek(long)} */
  private long newPos = 0;
  /** Current pos of object content */
  private long pos = -1;

  private InputStream internalStream;

  private final Counter readBytes;
  private final Counter readOperations;

  EcsSeekableInputStream(S3Client client, EcsURI uri, MetricsContext metrics) {
    this.client = client;
    this.uri = uri;
    this.readBytes = metrics.counter(FileIOMetricsContext.READ_BYTES, Unit.BYTES);
    this.readOperations = metrics.counter(FileIOMetricsContext.READ_OPERATIONS);
  }

  @Override
  public long getPos() {
    return newPos >= 0 ? newPos : pos;
  }

  @Override
  public void seek(long inputNewPos) {
    if (pos == inputNewPos) {
      return;
    }

    newPos = inputNewPos;
  }

  @Override
  public int read() throws IOException {
    checkAndUseNewPos();
    pos += 1;
    readBytes.increment();
    readOperations.increment();
    return internalStream.read();
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    checkAndUseNewPos();
    int delta = internalStream.read(b, off, len);
    pos += delta;
    readBytes.increment(delta);
    readOperations.increment();
    return delta;
  }

  private void checkAndUseNewPos() throws IOException {
    if (newPos < 0) {
      return;
    }

    if (newPos == pos) {
      newPos = -1;
      return;
    }

    if (internalStream != null) {
      internalStream.close();
    }

    pos = newPos;
    internalStream = client.readObjectStream(uri.bucket(), uri.name(), Range.fromOffset(pos));
    newPos = -1;
  }

  @Override
  public void close() throws IOException {
    if (internalStream != null) {
      internalStream.close();
    }
  }
}
