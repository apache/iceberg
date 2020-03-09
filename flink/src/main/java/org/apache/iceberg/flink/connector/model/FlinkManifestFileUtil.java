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

package org.apache.iceberg.flink.connector.model;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class FlinkManifestFileUtil {
  private FlinkManifestFileUtil() {}

  public static long getDataFileCount(List<FlinkManifestFile> flinkManifestFiles) {
    return flinkManifestFiles.stream().map(FlinkManifestFile::dataFileCount)
        .collect(Collectors.summingLong(l -> l));
  }

  public static long getRecordCount(List<FlinkManifestFile> flinkManifestFiles) {
    return flinkManifestFiles.stream().map(FlinkManifestFile::recordCount)
        .collect(Collectors.summingLong(l -> l));
  }

  public static long getByteCount(List<FlinkManifestFile> flinkManifestFiles) {
    return flinkManifestFiles.stream().map(FlinkManifestFile::byteCount)
        .collect(Collectors.summingLong(l -> l));
  }

  public static Long getLowWatermark(List<FlinkManifestFile> flinkManifestFiles) {
    Long min = null;
    for (FlinkManifestFile flinkManifestFile : flinkManifestFiles) {
      Long curr = flinkManifestFile.lowWatermark();
      if ((null == min) || (null != curr && min > curr)) {
        min = curr;
      }
    }
    return min;
  }

  public static Long getHighWatermark(List<FlinkManifestFile> flinkManifestFiles) {
    Long max = null;
    for (FlinkManifestFile flinkManifestFile : flinkManifestFiles) {
      Long curr = flinkManifestFile.highWatermark();
      if ((null == max) || (null != curr && max < curr)) {
        max = curr;
      }
    }
    return max;
  }

  public static String hashesListToString(List<String> hashes) {
    return Joiner.on(",").join(hashes);
  }

  public static List<String> hashesStringToList(String hashesStr) {
    if (Strings.isNullOrEmpty(hashesStr)) {
      return Collections.emptyList();
    } else {
      return Arrays.asList(hashesStr.split(","));
    }
  }
}
