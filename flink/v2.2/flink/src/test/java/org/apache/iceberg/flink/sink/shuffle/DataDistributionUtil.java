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
package org.apache.iceberg.flink.sink.shuffle;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.iceberg.SortKey;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

public class DataDistributionUtil {
  private DataDistributionUtil() {}

  private static final String CHARS =
      "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-.!?";

  /** Generate a random string with a given prefix and a random length up to maxLength. */
  public static String randomString(String prefix, int maxLength) {
    int length = ThreadLocalRandom.current().nextInt(maxLength);
    byte[] buffer = new byte[length];

    for (int i = 0; i < length; i += 1) {
      buffer[i] = (byte) CHARS.charAt(ThreadLocalRandom.current().nextInt(CHARS.length()));
    }

    return prefix + new String(buffer, StandardCharsets.UTF_8);
  }

  /**
   * return index if index == 0 && weightsUDF[index] > target (or) weightsUDF[index-1] <= target &&
   * weightsUDF[index] > target
   */
  public static int binarySearchIndex(long[] weightsCDF, long target) {
    Preconditions.checkArgument(
        target >= 0, "target weight must be non-negative: search target = %s", target);
    Preconditions.checkArgument(
        target < weightsCDF[weightsCDF.length - 1],
        "target weight is out of range: total weight = %s, search target = %s",
        weightsCDF[weightsCDF.length - 1],
        target);

    int start = 0;
    int end = weightsCDF.length - 1;
    while (start <= end) {
      int mid = (start + end) / 2;
      boolean leftOk = (mid == 0) || (weightsCDF[mid - 1] <= target);
      boolean rightOk = weightsCDF[mid] > target;
      if (leftOk && rightOk) {
        return mid;
      } else if (weightsCDF[mid] <= target) {
        start = mid + 1;
      } else {
        end = mid - 1;
      }
    }

    throw new IllegalStateException("should never reach here");
  }

  /** Key is the id string and value is the weight in long value. */
  public static NavigableMap<Integer, Long> longTailDistribution(
      long startingWeight,
      int longTailStartingIndex,
      int longTailLength,
      long longTailBaseWeight,
      double weightRandomJitterPercentage,
      double decayFactor) {

    NavigableMap<Integer, Long> weights = Maps.newTreeMap();

    // decay part
    long currentWeight = startingWeight;
    for (int index = 0; index < longTailStartingIndex; ++index) {
      double jitter = ThreadLocalRandom.current().nextDouble(weightRandomJitterPercentage / 100);
      long weight = (long) (currentWeight * (1.0 + jitter));
      weight = weight > 0 ? weight : 1;
      weights.put(index, weight);
      if (currentWeight > longTailBaseWeight) {
        currentWeight = (long) (currentWeight * decayFactor); // decay the weight by 40%
      }
    }

    // long tail part (flat with some random jitter)
    for (int index = longTailStartingIndex;
        index < longTailStartingIndex + longTailLength;
        ++index) {
      long longTailWeight =
          (long)
              (longTailBaseWeight
                  * ThreadLocalRandom.current().nextDouble(weightRandomJitterPercentage));
      longTailWeight = longTailWeight > 0 ? longTailWeight : 1;
      weights.put(index, longTailWeight);
    }

    return weights;
  }

  public static Map<SortKey, Long> mapStatisticsWithLongTailDistribution(
      NavigableMap<Integer, Long> weights, SortKey sortKey) {
    Map<SortKey, Long> mapStatistics = Maps.newHashMapWithExpectedSize(weights.size());
    weights.forEach(
        (id, weight) -> {
          SortKey sortKeyCopy = sortKey.copy();
          sortKeyCopy.set(0, id);
          mapStatistics.put(sortKeyCopy, weight);
        });

    return mapStatistics;
  }

  public static long[] computeCumulativeWeights(List<Integer> keys, Map<Integer, Long> weights) {
    long[] weightsCDF = new long[keys.size()];
    long totalWeight = 0;
    for (int i = 0; i < keys.size(); ++i) {
      totalWeight += weights.get(keys.get(i));
      weightsCDF[i] = totalWeight;
    }

    return weightsCDF;
  }

  public static byte[] uuidBytes(UUID uuid) {
    ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
    bb.putLong(uuid.getMostSignificantBits());
    bb.putLong(uuid.getLeastSignificantBits());
    return bb.array();
  }

  public static UUID[] reservoirSampleUUIDs(int sampleSize, int reservoirSize) {
    UUID[] reservoir = new UUID[reservoirSize];
    for (int i = 0; i < reservoirSize; ++i) {
      reservoir[i] = UUID.randomUUID();
    }

    ThreadLocalRandom random = ThreadLocalRandom.current();
    for (int i = reservoirSize; i < sampleSize; ++i) {
      int rand = random.nextInt(i + 1);
      if (rand < reservoirSize) {
        reservoir[rand] = UUID.randomUUID();
      }
    }

    Arrays.sort(reservoir);
    return reservoir;
  }

  public static UUID[] rangeBoundSampleUUIDs(UUID[] sampledUUIDs, int rangeBoundSize) {
    UUID[] rangeBounds = new UUID[rangeBoundSize];
    int step = sampledUUIDs.length / rangeBoundSize;
    for (int i = 0; i < rangeBoundSize; ++i) {
      rangeBounds[i] = sampledUUIDs[i * step];
    }
    Arrays.sort(rangeBounds);
    return rangeBounds;
  }
}
