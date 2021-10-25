/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.dell;

import com.emc.object.s3.S3Client;
import com.emc.object.s3.request.PutObjectRequest;
import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import org.apache.iceberg.io.PositionOutputStream;

/**
 * Use ECS append API to write data.
 */
public class EcsAppendOutputStream extends PositionOutputStream {

  private final S3Client client;

  private final EcsURI key;

  /**
   * Local bytes cache that avoid too many requests
   * <p>
   * Use {@link ByteBuffer} to maintain offset.
   */
  private final ByteBuffer localCache;

  /**
   * A marker for data file to put first part instead of append first part.
   */
  private boolean firstPart = true;

  /**
   * Pos for {@link PositionOutputStream}
   */
  private long pos;

  public EcsAppendOutputStream(S3Client client, EcsURI key, byte[] localCache) {
    this.client = client;
    this.key = key;
    this.localCache = ByteBuffer.wrap(localCache);
  }

  /**
   * Write a byte. If buffer is full, upload the buffer.
   */
  @Override
  public void write(int b) {
    if (!checkBuffer(1)) {
      flush();
    }
    localCache.put((byte) b);
    pos += 1;
  }

  /**
   * Write a byte.
   * If buffer is full, upload the buffer.
   * If buffer size &lt; input bytes, upload input bytes.
   */
  @Override
  public void write(byte[] b, int off, int len) {
    if (!checkBuffer(len)) {
      flush();
    }
    if (checkBuffer(len)) {
      localCache.put(b, off, len);
    } else {
      // if content > cache, directly flush itself.
      flushBuffer(b, off, len);
    }
    pos += len;
  }

  private boolean checkBuffer(int nextWrite) {
    return localCache.remaining() >= nextWrite;
  }

  private void flushBuffer(byte[] buffer, int offset, int length) {
    if (firstPart) {
      client.putObject(new PutObjectRequest(key.getBucket(), key.getName(),
          new ByteArrayInputStream(buffer, offset, length)));
      firstPart = false;
    } else {
      client.appendObject(key.getBucket(), key.getName(), new ByteArrayInputStream(buffer, offset, length));
    }
  }

  /**
   * Pos of the file
   */
  @Override
  public long getPos() {
    return pos;
  }

  /**
   * Write cached bytes if present.
   */
  @Override
  public void flush() {
    if (localCache.remaining() < localCache.capacity()) {
      localCache.flip();
      flushBuffer(localCache.array(), localCache.arrayOffset(), localCache.remaining());
      localCache.clear();
    }
  }

  /**
   * Trigger flush() when closing stream.
   */
  @Override
  public void close() {
    flush();
  }
}
