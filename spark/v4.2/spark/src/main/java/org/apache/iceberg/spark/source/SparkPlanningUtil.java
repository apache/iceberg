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
package org.apache.iceberg.spark.source;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.hadoop.Util;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.JavaHash;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;

class SparkPlanningUtil {

  public static final String[] NO_LOCATION_PREFERENCE = new String[0];

  private SparkPlanningUtil() {}

  public static String[][] fetchBlockLocations(
      FileIO io, List<? extends ScanTaskGroup<?>> taskGroups) {
    String[][] locations = new String[taskGroups.size()][];

    Tasks.range(taskGroups.size())
        .stopOnFailure()
        .executeWith(ThreadPools.getWorkerPool())
        .run(index -> locations[index] = Util.blockLocations(io, taskGroups.get(index)));

    return locations;
  }

  public static String[][] assignExecutors(
      List<? extends ScanTaskGroup<?>> taskGroups, List<String> executorLocations) {
    Map<Integer, JavaHash<StructLike>> partitionHashes = Maps.newHashMap();
    String[][] locations = new String[taskGroups.size()][];

    for (int index = 0; index < taskGroups.size(); index++) {
      locations[index] = assign(taskGroups.get(index), executorLocations, partitionHashes);
    }

    return locations;
  }

  private static String[] assign(
      ScanTaskGroup<?> taskGroup,
      List<String> executorLocations,
      Map<Integer, JavaHash<StructLike>> partitionHashes) {
    List<String> locations = Lists.newArrayList();

    for (ScanTask task : taskGroup.tasks()) {
      if (task.isFileScanTask()) {
        FileScanTask fileTask = task.asFileScanTask();
        PartitionSpec spec = fileTask.spec();
        if (spec.isPartitioned() && !fileTask.deletes().isEmpty()) {
          JavaHash<StructLike> partitionHash =
              partitionHashes.computeIfAbsent(spec.specId(), key -> partitionHash(spec));
          int partitionHashCode = partitionHash.hash(fileTask.partition());
          int index = Math.floorMod(partitionHashCode, executorLocations.size());
          String executorLocation = executorLocations.get(index);
          locations.add(executorLocation);
        }
      }
    }

    return locations.toArray(NO_LOCATION_PREFERENCE);
  }

  private static JavaHash<StructLike> partitionHash(PartitionSpec spec) {
    return JavaHash.forType(spec.partitionType());
  }
}
