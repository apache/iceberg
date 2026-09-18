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
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.ByteBuffers;

public class MumblingTestUtil {
  private MumblingTestUtil() {}

  /** Returns the serialized bytes of a Mumbling bitmap with only the first bit set. */
  public static byte[] onlyFirstBitSetBytes() {
    return ByteBuffers.toByteArray(build(sparse(0)));
  }

  static MumblingBitmap bitmap(Container... containers) {
    return new MumblingBitmap(build(containers));
  }

  static ByteBuffer build(Container... containers) {
    Preconditions.checkArgument(
        containers.length <= 8192, "Invalid container count (max 8192): %s", containers.length);

    int[] descriptors = new int[containers.length];
    int cardinality = 0;
    int sizeEstimate = 6;
    for (int i = 0; i < containers.length; i += 1) {
      descriptors[i] = containers[i].descriptor;
      cardinality += containers[i].cardinality;
      sizeEstimate += containers[i].bytes.length;
    }

    Preconditions.checkArgument(
        cardinality <= 2_097_152, "Invalid cardinality (max 2,097,152): %s", cardinality);

    sizeEstimate += PFOREncoding.estimateEncodedSize(containers.length);
    ByteBuffer buf = ByteBuffer.allocate(sizeEstimate);

    // header: version (1 byte), cardinality (3 bytes LE), container count (2 bytes LE)
    buf.put(0, (byte) 1);
    buf.put(1, (byte) (cardinality & 0xFF));
    buf.put(2, (byte) ((cardinality >>> 8) & 0xFF));
    buf.put(3, (byte) ((cardinality >>> 16) & 0xFF));
    buf.put(4, (byte) (containers.length & 0xFF));
    buf.put(5, (byte) ((containers.length >>> 8) & 0xFF));

    // write encoded descriptors
    int descriptorArraySize = PFOREncoding.encode(descriptors, 0, buf, 6, descriptors.length);

    // copy container bytes into the array
    int containerOffset = 6 + descriptorArraySize;
    for (Container spec : containers) {
      buf.put(containerOffset, spec.bytes);
      containerOffset += spec.bytes.length;
    }

    // the offset after the last container is the length
    buf.limit(containerOffset);

    return buf;
  }

  static Container sparse(int... positions) {
    byte[] bytes = new byte[positions.length];
    for (int i = 0; i < positions.length; i += 1) {
      if (i > 0) {
        Preconditions.checkArgument(
            positions[i] < 256, "Invalid position in container: %s", positions[i]);
        Preconditions.checkArgument(
            positions[i] > positions[i - 1],
            "Invalid sparse container: pos %s=%s >= pos %s=%s",
            i - 1,
            positions[i - 1],
            i,
            positions[i]);
      }

      bytes[i] = (byte) positions[i];
    }

    return new Container(bytes);
  }

  /** Descriptor + bytes for a dense container. */
  static Container dense(byte[] container) {
    Preconditions.checkArgument(container.length == 32, "Dense container must be 32 bytes");
    return new Container(container);
  }

  static class Container {
    private final byte[] bytes;
    private final int descriptor;
    private final int cardinality;

    Container(byte[] bytes) {
      this.bytes = bytes;
      this.descriptor = bytes.length;
      this.cardinality = cardinality(bytes);
    }

    private static int cardinality(byte[] bytes) {
      if (bytes.length < 32) {
        return bytes.length;
      } else if (bytes.length == 32) {
        int setBits = 0;
        for (byte b : bytes) {
          setBits += Integer.bitCount(b & 0xFF);
        }

        Preconditions.checkArgument(
            setBits > 31, "Invalid dense container: %s values should be sparse", setBits);

        return setBits;
      } else {
        throw new IllegalArgumentException("Invalid container: longer than 32 bytes");
      }
    }
  }
}
