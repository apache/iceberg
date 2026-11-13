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
package org.apache.iceberg.tornadovm;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Iterator;
import java.util.NavigableSet;

/**
 * Apache DataSketches' compact theta sketch (serialization version 3), from the smallest distinct
 * hashes of a column, computed elsewhere (on the device). DataSketches builds a sketch only from
 * values it hashes itself, so the bytes are laid out here, as {@code CompactSketch.toByteArray()}
 * lays out an ordered compact sketch: an empty, exact or estimating preamble, then the hashes.
 */
final class ThetaSketches {
  private ThetaSketches() {}

  private static final byte SERIAL_VERSION = 3;
  private static final byte FAMILY_COMPACT = 3;
  private static final byte FLAG_READ_ONLY = 2;
  private static final byte FLAG_EMPTY = 4;
  private static final byte FLAG_COMPACT = 8;
  private static final byte FLAG_ORDERED = 16;
  private static final short SEED_HASH =
      org.apache.datasketches.thetacommon.ThetaUtil.computeSeedHash(
          org.apache.datasketches.thetacommon.ThetaUtil.DEFAULT_UPDATE_SEED);

  /**
   * A compact ordered sketch of nominal size {@code entries}, from up to {@code entries + 1}
   * smallest distinct hashes (ascending): at most {@code entries} of them it holds exactly; past
   * that it holds the {@code entries} smallest, and theta is the next one.
   */
  static byte[] compact(NavigableSet<Long> smallest, int entries) {
    boolean estimating = smallest.size() > entries;
    int count = estimating ? entries : smallest.size();
    int preambleLongs = smallest.isEmpty() ? 1 : estimating ? 3 : 2;
    ByteBuffer out =
        ByteBuffer.allocate(8 * (preambleLongs + count)).order(ByteOrder.LITTLE_ENDIAN);
    byte flags = FLAG_READ_ONLY | FLAG_COMPACT | FLAG_ORDERED;
    if (smallest.isEmpty()) {
      flags |= FLAG_EMPTY;
    }
    out.put((byte) preambleLongs).put(SERIAL_VERSION).put(FAMILY_COMPACT).put((byte) 0);
    out.put((byte) 0).put(flags).putShort(SEED_HASH);
    if (preambleLongs >= 2) {
      out.putInt(count).putFloat(1.0f);
    }
    Iterator<Long> hashes = smallest.iterator();
    if (estimating) {
      long theta = 0;
      Iterator<Long> scan = smallest.iterator();
      for (int i = 0; i <= entries; i++) {
        theta = scan.next();
      }
      out.putLong(theta);
    }
    for (int i = 0; i < count; i++) {
      out.putLong(hashes.next());
    }
    return out.array();
  }
}
