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

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Randomized roundtrip tests for {@link PFOREncoding}.
 *
 * <p>Each test case embeds a fixed seed so that failures are reproducible. The cases cover:
 *
 * <ul>
 *   <li><b>Uniform distributions</b> at several value ranges to exercise different primary bit
 *       widths ({@code b1}): 0-1 → b1=1, 0-3 → b1=2, 0-31 → b1=5, 0-255 → b1=8.
 *   <li><b>Size boundaries</b> around 8 (the pack/unpack group size) and 256 (the chunk size), to
 *       exercise the remainder logic and multi-chunk splitting.
 *   <li><b>Sparse distributions</b> — mostly-uniform base values with a small fraction of outliers
 *       — to exercise the exception encoding path, where a large {@code b2} is needed.
 *   <li><b>Non-zero minimum</b> values, to exercise the {@code m} subtraction.
 * </ul>
 */
public class TestPFOREncodingRandom {

  // ---------------------------------------------------------------------------
  // Uniform distributions at different value ranges
  // ---------------------------------------------------------------------------

  /**
   * Each row: (description, count, seed, maxValue). Values are drawn uniformly from [0, maxValue].
   * The choice of maxValue determines which primary bit width {@code b1} the encoder will select.
   */
  static Stream<Arguments> uniformCases() {
    return Stream.of(
        // 1-bit range (b1=1): values in {0, 1}
        Arguments.of("1-bit range, size=1",    1,   0x1a2b3c4dL, 1),
        Arguments.of("1-bit range, size=7",    7,   0x5e6f7a8bL, 1),
        Arguments.of("1-bit range, size=8",    8,   0xdeadbeefL, 1),
        Arguments.of("1-bit range, size=9",    9,   0xcafebabeL, 1),
        Arguments.of("1-bit range, size=256",  256, 0xf00dfa11L, 1),

        // 2-bit range (b1=2): values in [0, 3]
        Arguments.of("2-bit range, size=7",    7,   0x1337c0deL, 3),
        Arguments.of("2-bit range, size=8",    8,   0xa5a5a5a5L, 3),
        Arguments.of("2-bit range, size=9",    9,   0x0badf00dL, 3),
        Arguments.of("2-bit range, size=100",  100, 0x12345678L, 3),
        Arguments.of("2-bit range, size=256",  256, 0x87654321L, 3),

        // 3-bit range (b1=3): values in [0, 7]
        Arguments.of("3-bit range, size=7",    7,   0xabcdef01L, 7),
        Arguments.of("3-bit range, size=24",   24,  0x01fedcbaL, 7),
        Arguments.of("3-bit range, size=256",  256, 0xbeefcafeL, 7),

        // 5-bit range (b1=5): values in [0, 31] — typical for Mumbling descriptor bytes
        Arguments.of("5-bit range, size=7",    7,   0x11223344L, 31),
        Arguments.of("5-bit range, size=8",    8,   0x44332211L, 31),
        Arguments.of("5-bit range, size=63",   63,  0xaabbccddL, 31),
        Arguments.of("5-bit range, size=64",   64,  0xddccbbaaL, 31),
        Arguments.of("5-bit range, size=256",  256, 0x55667788L, 31),

        // Full byte range (b1=8 or b1 chosen by cost): values in [0, 255]
        Arguments.of("full range, size=1",     1,   0x99aabbccL, 255),
        Arguments.of("full range, size=7",     7,   0xcc998877L, 255),
        Arguments.of("full range, size=8",     8,   0x13572468L, 255),
        Arguments.of("full range, size=9",     9,   0x24681357L, 255),
        Arguments.of("full range, size=15",    15,  0xfedcba98L, 255),
        Arguments.of("full range, size=16",    16,  0x89abcdefL, 255),
        Arguments.of("full range, size=100",   100, 0x5a5a5a5aL, 255),
        Arguments.of("full range, size=255",   255, 0xa1b2c3d4L, 255),
        Arguments.of("full range, size=256",   256, 0xd4c3b2a1L, 255),
        Arguments.of("full range, size=257",   257, 0x1f2e3d4cL, 255),
        Arguments.of("full range, size=512",   512, 0x4c3d2e1fL, 255),
        Arguments.of("full range, size=513",   513, 0xface0ffL,  255));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("uniformCases")
  public void testUniformRandom(String name, int count, long seed, int maxValue) {
    assertRoundtrip(PFOREncodingTestUtils.uniform(count, seed, maxValue));
  }

  // ---------------------------------------------------------------------------
  // Sparse distributions — tests exception encoding
  //
  // Most values are drawn from a narrow base range (0-3), but a fraction are
  // replaced with full-range values (0-255). This creates chunks where b1 is
  // small but e > 0, which exercises the exception path with b2 > 0.
  // ---------------------------------------------------------------------------

  /**
   * Each row: (description, count, seed, exceptionProbabilityPct). Values are 0-3 except that each
   * position has a {@code exceptionProbabilityPct}% chance of becoming a full-range outlier
   * (0-255).
   */
  static Stream<Arguments> sparseCases() {
    return Stream.of(
        Arguments.of("sparse 10%, size=7",    7,   0x2a3b4c5dL, 10),
        Arguments.of("sparse 10%, size=8",    8,   0x3c4d5e6fL, 10),
        Arguments.of("sparse 10%, size=9",    9,   0x4e5f607aL, 10),
        Arguments.of("sparse 10%, size=100",  100, 0x6b7c8d9eL, 10),
        Arguments.of("sparse 10%, size=256",  256, 0x8d9eafb0L, 10),
        Arguments.of("sparse 25%, size=256",  256, 0xc1d2e3f4L, 25),
        Arguments.of("sparse 50%, size=256",  256, 0xf4e3d2c1L, 50),
        Arguments.of("sparse 10%, size=512",  512, 0x10293847L, 10));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("sparseCases")
  public void testSparseRandom(String name, int count, long seed, int exceptionPct) {
    assertRoundtrip(PFOREncodingTestUtils.sparse(count, seed, exceptionPct));
  }

  // ---------------------------------------------------------------------------
  // Non-zero minimum — tests the m (minimum subtraction) mechanism
  //
  // Values are drawn from a range that does not include 0, so m > 0 and the
  // encoder must subtract it before packing. After decoding, m is added back.
  // ---------------------------------------------------------------------------

  /**
   * Each row: (description, count, seed, minValue, range). Values are drawn uniformly from
   * [minValue, minValue + range].
   */
  static Stream<Arguments> offsetCases() {
    return Stream.of(
        Arguments.of("offset m=100 range=3, size=8",   8,   0x31415926L, 100, 3),
        Arguments.of("offset m=100 range=3, size=256", 256, 0x27182818L, 100, 3),
        Arguments.of("offset m=50  range=31, size=64", 64,  0x16180339L,  50, 31),
        Arguments.of("offset m=200 range=55, size=100",100, 0x14142135L, 200, 55),
        Arguments.of("offset m=128 range=127, size=256",256,0x17320508L, 128, 127));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("offsetCases")
  public void testOffsetRandom(String name, int count, long seed, int minValue, int range) {
    assertRoundtrip(PFOREncodingTestUtils.withOffset(count, seed, minValue, range));
  }

  // ---------------------------------------------------------------------------
  // Helper
  // ---------------------------------------------------------------------------

  private static void assertRoundtrip(int[] values) {
    ByteBuffer encoded = PFOREncoding.encode(values);
    int[] decoded = PFOREncoding.decode(encoded, values.length);
    assertThat(decoded).isEqualTo(values);
  }
}
