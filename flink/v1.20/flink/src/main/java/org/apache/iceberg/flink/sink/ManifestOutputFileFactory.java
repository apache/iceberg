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
package org.apache.iceberg.flink.sink;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Strings;

class ManifestOutputFileFactory {
  // Users could define their own flink manifests directory by setting this value in table
  // properties.
  @VisibleForTesting static final String FLINK_MANIFEST_LOCATION = "flink.manifests.location";
  private final Supplier<Table> tableSupplier;
  private final Map<String, String> props;
  private final String flinkJobId;
  private final String operatorUniqueId;
  private final int subTaskId;
  private final long attemptNumber;
  private final AtomicInteger fileCount = new AtomicInteger(0);

  ManifestOutputFileFactory(
      Supplier<Table> tableSupplier,
      Map<String, String> props,
      String flinkJobId,
      String operatorUniqueId,
      int subTaskId,
      long attemptNumber) {
    this.tableSupplier = tableSupplier;
    this.props = props;
    this.flinkJobId = flinkJobId;
    this.operatorUniqueId = operatorUniqueId;
    this.subTaskId = subTaskId;
    this.attemptNumber = attemptNumber;
  }

  private String generatePath(long checkpointId) {
    return FileFormat.AVRO.addExtension(
        String.format(
            "%s-%s-%05d-%d-%d-%05d",
            flinkJobId,
            operatorUniqueId,
            subTaskId,
            attemptNumber,
            checkpointId,
            fileCount.incrementAndGet()));
  }

  OutputFile create(long checkpointId) {
    String flinkManifestDir = props.get(FLINK_MANIFEST_LOCATION);
    TableOperations ops = ((HasTableOperations) tableSupplier.get()).operations();

    String newManifestFullPath;
    if (Strings.isNullOrEmpty(flinkManifestDir)) {
      // User don't specify any flink manifest directory, so just use the default metadata path.
      newManifestFullPath = ops.metadataFileLocation(generatePath(checkpointId));
    } else {
      newManifestFullPath =
          String.format("%s/%s", stripTrailingSlash(flinkManifestDir), generatePath(checkpointId));
    }

    return tableSupplier.get().io().newOutputFile(newManifestFullPath);
  }

  private static String stripTrailingSlash(String path) {
    String result = path;
    while (result.endsWith("/")) {
      result = result.substring(0, result.length() - 1);
    }
    return result;
  }
}
