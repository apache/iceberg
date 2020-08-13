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

package org.apache.iceberg.util;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

public class ArrayUtil {
  private ArrayUtil() {
  }

  public static List<Integer> toIntList(int[] ints) {
    if (ints != null) {
      return IntStream.of(ints).boxed().collect(Collectors.toList());
    } else {
      return null;
    }
  }

  public static int[] toIntArray(List<Integer> ints) {
    if (ints != null) {
      return ints.stream().mapToInt(v -> v).toArray();
    } else {
      return null;
    }
  }

  public static List<Long> toLongList(long[] longs) {
    if (longs != null) {
      return LongStream.of(longs).boxed().collect(Collectors.toList());
    } else {
      return null;
    }
  }

  public static long[] toLongArray(List<Long> longs) {
    if (longs != null) {
      return longs.stream().mapToLong(v -> v).toArray();
    } else {
      return null;
    }
  }
}
