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
package org.apache.iceberg.flink.source;

import java.util.function.Supplier;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.FlinkConfigOptions;
import org.apache.iceberg.hadoop.Util;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

class SourceUtil {
  private SourceUtil() {}

  static boolean isLocalityEnabled(
      Table table, ReadableConfig readableConfig, Boolean exposeLocality) {
    Boolean localityEnabled =
        exposeLocality != null
            ? exposeLocality
            : readableConfig.get(FlinkConfigOptions.TABLE_EXEC_ICEBERG_EXPOSE_SPLIT_LOCALITY_INFO);

    if (localityEnabled != null && !localityEnabled) {
      return false;
    }

    return Util.mayHaveBlockLocations(table.io(), table.location());
  }

  /**
   * Infer source parallelism.
   *
   * @param readableConfig Flink config.
   * @param splitCountProvider Split count supplier. As the computation may involve expensive split
   *     discover, lazy evaluation is performed if inferring parallelism is enabled.
   * @param limitCount limited output count.
   */
  static int inferParallelism(
      ReadableConfig readableConfig, long limitCount, Supplier<Integer> splitCountProvider) {
    int parallelism =
        readableConfig.get(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM);
    if (readableConfig.get(FlinkConfigOptions.TABLE_EXEC_ICEBERG_INFER_SOURCE_PARALLELISM)) {
      int maxInferParallelism =
          readableConfig.get(FlinkConfigOptions.TABLE_EXEC_ICEBERG_INFER_SOURCE_PARALLELISM_MAX);
      Preconditions.checkState(
          maxInferParallelism >= 1,
          FlinkConfigOptions.TABLE_EXEC_ICEBERG_INFER_SOURCE_PARALLELISM_MAX.key()
              + " cannot be less than 1");
      parallelism = Math.min(splitCountProvider.get(), maxInferParallelism);
    }

    if (limitCount > 0) {
      int limit = limitCount >= Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) limitCount;
      parallelism = Math.min(parallelism, limit);
    }

    // parallelism must be positive.
    parallelism = Math.max(1, parallelism);
    return parallelism;
  }
}
