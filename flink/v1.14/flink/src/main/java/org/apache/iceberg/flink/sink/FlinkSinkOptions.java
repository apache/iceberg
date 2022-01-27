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

import org.apache.iceberg.TableProperties;

/**
 * Flink iceberg sinker options
 */
public class FlinkSinkOptions {

  private FlinkSinkOptions() {
  }

  public static final String STREAMING_REWRITE_ENABLE = "flink.rewrite.enable";
  public static final boolean STREAMING_REWRITE_ENABLE_DEFAULT = false;

  public static final String STREAMING_REWRITE_PARALLELISM = "flink.rewrite.parallelism";
  public static final Integer STREAMING_REWRITE_PARALLELISM_DEFAULT = null;  // use flink job default parallelism

  public static final String STREAMING_REWRITE_CASE_SENSITIVE = "flink.rewrite.case-sensitive";
  public static final boolean STREAMING_REWRITE_CASE_SENSITIVE_DEFAULT = false;

  /**
   * The output file size attempt to generate when rewriting files.
   * <p>
   * Defaults will use the {@link TableProperties#WRITE_TARGET_FILE_SIZE_BYTES} value.
   */
  public static final String STREAMING_REWRITE_TARGET_FILE_SIZE_BYTES = "flink.rewrite.target-file-size-bytes";

  /**
   * Adjusts files which will be considered for rewriting.
   * Files smaller than this value will be considered for rewriting.
   * <p>
   * Defaults to 75% of the target file size
   */
  public static final String STREAMING_REWRITE_MIN_FILE_SIZE_BYTES = "flink.rewrite.min-file-size-bytes";
  public static final double STREAMING_REWRITE_MIN_FILE_SIZE_DEFAULT_RATIO = 0.75d;

  /**
   * Adjusts files which will be considered for rewriting.
   * Files larger than this value will be considered for rewriting.
   * <p>
   * Defaults to 180% of the target file size
   */
  public static final String STREAMING_REWRITE_MAX_FILE_SIZE_BYTES = "flink.rewrite.max-file-size-bytes";
  public static final double STREAMING_REWRITE_MAX_FILE_SIZE_DEFAULT_RATIO = 1.80d;

  /**
   * The minimum number of files that need to be in a file group for it to be considered for rewriting,
   * if the total size of that group is reach the {@link #STREAMING_REWRITE_TARGET_FILE_SIZE_BYTES} or
   * the total number of files of that group is reach to the {@link #STREAMING_REWRITE_MAX_GROUP_FILES}.
   * <p>
   * Defaults to 2 files, which is mean at less 2 files in a file group will be considered for rewriting.
   */
  public static final String STREAMING_REWRITE_MIN_GROUP_FILES = "flink.rewrite.min-group-files";
  public static final int STREAMING_REWRITE_MIN_GROUP_FILES_DEFAULT = 2;

  /**
   * The maximum number of files that allow to be in a file group for it to be considered for rewriting.
   * Once the total number of files of that group is reach to this value, the file group will be rewritten
   * regardless of whether the total size of that group reaches the {@link #STREAMING_REWRITE_TARGET_FILE_SIZE_BYTES}.
   * <p>
   * Defaults to Integer.MAX_VALUE, which means this feature is not enabled by default.
   */
  public static final String STREAMING_REWRITE_MAX_GROUP_FILES = "flink.rewrite.max-group-files";
  public static final int STREAMING_REWRITE_MAX_GROUP_FILES_DEFAULT = Integer.MAX_VALUE;

  /**
   * The maximum number of commits will be wait for a file group. If no more file append to this file group after
   * this number of commits, this file group will be rewritten regardless of whether the total size of that group
   * reaches the {@link #STREAMING_REWRITE_TARGET_FILE_SIZE_BYTES}.
   * <p>
   * Defaults to Integer.MAX_VALUE, which means this feature is not enabled by default.
   */
  public static final String STREAMING_REWRITE_MAX_WAITING_COMMITS = "flink.rewrite.nums-of-commit-after-append";
  public static final int STREAMING_REWRITE_MAX_WAITING_COMMITS_DEFAULT = Integer.MAX_VALUE;

}
