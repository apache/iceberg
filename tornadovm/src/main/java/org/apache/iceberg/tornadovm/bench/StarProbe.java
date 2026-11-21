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
package org.apache.iceberg.tornadovm.bench;

import java.util.List;
import java.util.Locale;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.tornadovm.DeviceMergeOnReadScan;
import org.apache.iceberg.tornadovm.DeviceValue;
import org.apache.iceberg.tornadovm.join.DeviceDimension;
import org.apache.iceberg.tornadovm.join.StarAggregation;

/**
 * Device time of a star aggregate by the shape of its joins:
 *
 * <pre>
 *   StarProbe &lt;table&gt; &lt;runs&gt; &lt;value&gt; &lt;column:first:keys:keepEvery:codes&gt;...
 * </pre>
 *
 * <p>Each {@code column:first:keys:keepEvery:codes} is a dimension on a fact column over keys
 * {@code first..first+keys-1} keeping every {@code keepEvery}-th key and grouping by {@code key %
 * codes} (no attribute when codes is 0).
 */
public final class StarProbe {
  private StarProbe() {}

  public static void main(String[] args) throws Exception {
    List<DeviceDimension> dimensions = Lists.newArrayList();
    List<DeviceDimension.Key> keys = Lists.newArrayList();
    for (int a = 3; a < args.length; a++) {
      List<String> parts = Splitter.on(':').splitToList(args[a]);
      int first = Integer.parseInt(parts.get(1));
      int count = Integer.parseInt(parts.get(2));
      int every = Integer.parseInt(parts.get(3));
      int codes = Integer.parseInt(parts.get(4));
      boolean[] keep = new boolean[count];
      int[] code = new int[count];
      for (int k = 0; k < count; k++) {
        keep[k] = k % every == 0;
        code[k] = codes == 0 ? 0 : k % codes;
      }
      DeviceDimension dimension = DeviceDimension.of(parts.get(0), first, keep);
      if (codes > 0) {
        keys.add(DeviceDimension.Key.attribute(dimensions.size(), 0));
        dimension = dimension.withAttribute(code);
      }
      dimensions.add(dimension);
    }
    try (DeviceMergeOnReadScan scan =
        DeviceMergeOnReadScan.of(MorTables.load(args[0]), Expressions.alwaysTrue())) {
      int runs = Integer.parseInt(args[1]);
      for (int run = 0; run < runs; run++) {
        long start = System.nanoTime();
        int size =
            StarAggregation.run(scan, dimensions, keys, ImmutableList.of(DeviceValue.of(args[2])))
                .groups()
                .size();
        System.out.printf(
            Locale.ROOT,
            "run=%d ms=%.1f groups=%d | %s%n",
            run,
            (System.nanoTime() - start) / 1e6,
            size,
            scan.metrics());
      }
    }
  }
}
