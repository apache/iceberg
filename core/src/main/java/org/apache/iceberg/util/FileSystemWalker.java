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
import java.util.function.Predicate;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.SupportsPrefixOperations;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/**
 * Utility class for recursively traversing file systems and identifying hidden paths. Provides
 * methods to list files recursively while filtering out hidden paths based on specified criteria.
 */
public class FileSystemWalker {

  private FileSystemWalker() {}

  /**
   * Recursively lists files in the specified directory that satisfy the given conditions.
   *
   * @param io File system interface supporting prefix operations
   * @param dir Base directory to start recursive listing
   * @param predicate Additional filter condition for files
   * @param pathFilter Filter to identify hidden paths
   * @return List to collect matching file locations
   */
  public static List<String> listDirRecursively(
      SupportsPrefixOperations io,
      String dir,
      Predicate<FileInfo> predicate,
      PathFilter pathFilter) {
    List<String> matchingFiles = Lists.newArrayList();
    String listPath = dir;
    if (!dir.endsWith("/")) {
      listPath = dir + "/";
    }

    Iterable<FileInfo> files = io.listPrefix(listPath);
    for (org.apache.iceberg.io.FileInfo file : files) {
      Path path = new Path(file.location());
      if (!isHiddenPath(dir, path, pathFilter) && predicate.test(file)) {
        matchingFiles.add(file.location());
      }
    }

    return matchingFiles;
  }

  /**
   * Determines if a path is hidden by checking its hierarchy against the base directory.
   *
   * @param baseDir The root directory to use as the stopping point for recursion
   * @param path The path to check for hidden status
   * @param pathFilter Filter used to evaluate path visibility
   * @return {@code true} if the path is hidden, {@code false} otherwise
   */
  public static boolean isHiddenPath(String baseDir, Path path, PathFilter pathFilter) {
    boolean isHiddenPath = false;
    Path currentPath = path;
    while (currentPath.getParent().toString().contains(baseDir)) {
      if (!pathFilter.accept(currentPath)) {
        isHiddenPath = true;
        break;
      }

      currentPath = currentPath.getParent();
    }

    return isHiddenPath;
  }
}
