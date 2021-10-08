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

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SmallFileUtil {
  private static final Logger LOG = LoggerFactory.getLogger(SmallFileUtil.class);

  private SmallFileUtil() {

  }

  /**
   * Returns whether small files should be merged.
   */
  public static boolean shouldMergeSmallFiles(Table table) {
    boolean shouldMerge = false;

    long currentTimeMillis = System.currentTimeMillis();
    Map<String, String> props = table.properties();
    boolean autoMergeEnable = PropertyUtil.propertyAsBoolean(
        props,
        TableProperties.WRITE_FLINK_AUTO_COMPACT_ENABLED,
        TableProperties.WRITE_FLINK_AUTO_COMPACT_ENABLED_DEFAULT);

    long mergeIntervalMillis = PropertyUtil.propertyAsLong(
        props,
        TableProperties.WRITE_FLINK_COMPACT_INTERVAL_MS,
        TableProperties.WRITE_FLINK_COMPACT_INTERVAL_MS_DEFAULT);

    long lastCommittedTimestamp = PropertyUtil.propertyAsLong(
        props,
        TableProperties.WRITE_FLINK_COMPACT_LAST_REWRITE_MS,
        TableProperties.WRITE_FLINK_COMPACT_LAST_REWRITE_MS_DEFAULT);

    long smallFileThreshold = PropertyUtil.propertyAsLong(
        props,
        TableProperties.WRITE_FLINK_COMPACT_SMALL_FILE_NUMS,
        TableProperties.WRITE_FLINK_COMPACT_SMALL_FILE_NUMS_DEFAULT);

    LOG.info("Summary: actual compact interval: {}s, compact.auto.enabled: {}, " +
            "compact.interval: {}s, current time: {}, last compact time: {}, small-file-nums: {}",
        (currentTimeMillis - lastCommittedTimestamp) / 1000,
        autoMergeEnable,
        mergeIntervalMillis / 1000,
        new Date(currentTimeMillis),
        new Date(lastCommittedTimestamp),
        smallFileThreshold);

    if (autoMergeEnable && (currentTimeMillis - lastCommittedTimestamp >= mergeIntervalMillis ||
        smallFileNums(table) >= smallFileThreshold)) {
      LOG.info("Should compact small files");
      shouldMerge = true;
    }
    return shouldMerge;
  }

  /**
   * Returns the number of small files in current snapshot.
   */
  public static long smallFileNums(Table table) {
    Map<String, String> props = table.properties();
    long smallFileSize = PropertyUtil.propertyAsLong(
        props,
        TableProperties.WRITE_FLINK_COMPACT_SMALL_FILE_SIZE_BYTES,
        TableProperties.WRITE_FLINK_COMPACT_SMALL_FILE_SIZE_BYTES_DEFAULT);

    CloseableIterable<FileScanTask> tasks = table.newScan().ignoreResiduals().planFiles();
    List<DataFile> dataFiles = Lists.newArrayList(CloseableIterable.transform(tasks, FileScanTask::file));
    List<DataFile> filterDataFiles = dataFiles
        .stream()
        .filter(file -> file.fileSizeInBytes() < smallFileSize)
        .collect(Collectors.toList());
    LOG.info("small-file-size-bytes: {}, actual small files numbers: {}", smallFileSize, filterDataFiles.size());

    return filterDataFiles.size();
  }
}
