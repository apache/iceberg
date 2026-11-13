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

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.tornadovm.DeviceMergeOnReadScan;
import org.apache.iceberg.tornadovm.DeviceValue;

/**
 * Device time of one grouped aggregate per column set, to see what a column costs to read:
 *
 * <pre>
 *   ColumnProbe &lt;table&gt; &lt;runs&gt; &lt;group,columns|-&gt; &lt;value,columns&gt; [&lt;group|-&gt; &lt;values&gt;]...
 * </pre>
 */
public final class ColumnProbe {
  private ColumnProbe() {}

  public static void main(String[] args) throws Exception {
    try (DeviceMergeOnReadScan scan =
        DeviceMergeOnReadScan.of(MorTables.load(args[0]), Expressions.alwaysTrue())) {
      int runs = Integer.parseInt(args[1]);
      for (int a = 2; a + 1 < args.length; a += 2) {
        List<String> groups =
            "-".equals(args[a]) ? ImmutableList.of() : Arrays.asList(args[a].split(","));
        List<DeviceValue> values =
            Arrays.stream(args[a + 1].split(",")).map(DeviceValue::of).toList();
        for (int run = 0; run < runs; run++) {
          long start = System.nanoTime();
          int size = scan.aggregate(groups, values).groups().size();
          System.out.printf(
              Locale.ROOT,
              "group=%s value=%s run=%d ms=%.1f groups=%d | %s%n",
              groups,
              args[a + 1],
              run,
              (System.nanoTime() - start) / 1e6,
              size,
              scan.metrics());
        }
      }
    }
  }
}
