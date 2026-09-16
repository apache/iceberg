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
package org.apache.iceberg.mumbling;

import java.nio.ByteBuffer;
import org.apache.iceberg.ManifestBitmap;
import org.apache.iceberg.util.ByteBuffers;

public class MumblingBitmapTestUtil {
  private MumblingBitmapTestUtil() {}

  /** Returns the serialized bytes of a Mumbling bitmap with bit 0 set. */
  public static byte[] bitmapBytes() {
    byte[] container = {0};
    int[] descriptors = {container.length};

    ByteBuffer buffer =
        ByteBuffer.allocate(6 + PFOREncoding.estimateEncodedSize(1) + container.length);
    buffer.put(0, (byte) 1);
    buffer.put(1, (byte) 1);
    buffer.put(2, (byte) 0);
    buffer.put(3, (byte) 0);
    buffer.put(4, (byte) 1);
    buffer.put(5, (byte) 0);
    int descriptorSize = PFOREncoding.encode(descriptors, 0, buffer, 6, 1);
    buffer.put(6 + descriptorSize, container);
    buffer.limit(6 + descriptorSize + container.length);

    return ByteBuffers.toByteArray(buffer);
  }

  /** Returns a Mumbling bitmap with bit 0 set. */
  public static ManifestBitmap bitmap() {
    return MumblingBitmaps.read(ByteBuffer.wrap(bitmapBytes()));
  }
}
