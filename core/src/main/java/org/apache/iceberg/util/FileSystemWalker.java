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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Predicate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
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
    listDirRecursively(io, dir, predicate, pathFilter, matchingFiles::add);
    return matchingFiles;
  }

  /**
   * Recursively lists files in the specified directory that satisfy the given conditions.
   *
   * @param io File system interface supporting prefix operations
   * @param dir Base directory to start recursive listing
   * @param predicate Additional filter condition for files
   * @param pathFilter Filter to identify hidden paths
   * @param consumer Consumer to accept matching file locations
   */
  public static void listDirRecursively(
      SupportsPrefixOperations io,
      String dir,
      Predicate<FileInfo> predicate,
      PathFilter pathFilter,
      Consumer<String> consumer) {
    String listPath = dir;
    if (!dir.endsWith("/")) {
      listPath = dir + "/";
    }

    Iterable<FileInfo> files = io.listPrefix(listPath);
    for (org.apache.iceberg.io.FileInfo file : files) {
      Path path = new Path(file.location());
      if (!isHiddenPath(dir, path, pathFilter) && predicate.test(file)) {
        consumer.accept(file.location());
      }
    }
  }

  public static void listDirRecursivelyWithHadoop(
      String dir,
      Predicate<FileStatus> predicate,
      Configuration conf,
      PathFilter pathFilter,
      Consumer<String> consumer) {
    processDirectoryWithHadoop(
        dir,
        predicate,
        conf,
        pathFilter,
        consumer,
        Integer.MAX_VALUE,
        Integer.MAX_VALUE,
        Lists.newArrayList());
  }

  /**
   * Recursively traverses the specified directory using Hadoop API to collect file paths that meet
   * the conditions.
   *
   * <p>This method provides depth control and subdirectory quantity limitation:
   *
   * <ul>
   *   <li>Stops traversal when maximum recursion depth is reached and adds current directory to
   *       pending list
   *   <li>Stops traversal when number of direct subdirectories exceeds threshold and adds
   *       subdirectories to pending list
   * </ul>
   *
   * @param dir The starting directory path to traverse
   * @param predicate File filter condition, only files satisfying this condition will be collected
   * @param conf Hadoop conf
   * @param maxDepth Maximum recursion depth limit
   * @param maxDirectSubDirs Upper limit of subdirectories that can be processed directly
   * @param remainingSubDirs Output parameter for storing unprocessed directory paths
   * @param pathFilter Path filter for excluding special directories (e.g. hidden paths)
   * @param matchingFiles List of qualified file paths
   */
  public static void listDirRecursivelyWithHadoop(
      String dir,
      Predicate<FileStatus> predicate,
      Configuration conf,
      int maxDepth,
      int maxDirectSubDirs,
      List<String> remainingSubDirs,
      PathFilter pathFilter,
      List<String> matchingFiles) {
    listDirRecursivelyWithHadoop(
        dir,
        predicate,
        conf,
        maxDepth,
        maxDirectSubDirs,
        remainingSubDirs,
        pathFilter,
        matchingFiles::add);
  }

  /**
   * Recursively traverses the specified directory using Hadoop API to collect file paths that meet
   * the conditions.
   *
   * <p>This method provides depth control and subdirectory quantity limitation:
   *
   * <ul>
   *   <li>Stops traversal when maximum recursion depth is reached and adds current directory to
   *       pending list
   *   <li>Stops traversal when number of direct subdirectories exceeds threshold and adds
   *       subdirectories to pending list
   * </ul>
   *
   * @param dir The starting directory path to traverse
   * @param predicate File filter condition, only files satisfying this condition will be collected
   * @param conf Hadoop conf
   * @param maxDepth Maximum recursion depth limit
   * @param maxDirectSubDirs Upper limit of subdirectories that can be processed directly
   * @param remainingSubDirs Output parameter for storing unprocessed directory paths
   * @param pathFilter Path filter for excluding special directories (e.g. hidden paths)
   * @param consumer Consumer for collecting qualified file paths
   */
  public static void listDirRecursivelyWithHadoop(
      String dir,
      Predicate<FileStatus> predicate,
      Configuration conf,
      int maxDepth,
      int maxDirectSubDirs,
      List<String> remainingSubDirs,
      PathFilter pathFilter,
      Consumer<String> consumer) {
    processDirectoryWithHadoop(
        dir, predicate, conf, pathFilter, consumer, maxDepth, maxDirectSubDirs, remainingSubDirs);
  }

  private static void processDirectoryWithHadoop(
      String dir,
      Predicate<FileStatus> predicate,
      Configuration conf,
      PathFilter pathFilter,
      Consumer<String> consumer,
      int maxDepth,
      int maxDirectSubDirs,
      List<String> remainingSubDirs) {

    if (maxDepth <= 0) {
      remainingSubDirs.add(dir);
      return;
    }

    try {
      Path path = new Path(dir);
      FileSystem fs = path.getFileSystem(conf);
      List<String> subDirs = Lists.newArrayList();

      for (FileStatus file : fs.listStatus(path, pathFilter)) {
        if (file.isDirectory()) {
          subDirs.add(file.getPath().toString());
        } else if (file.isFile() && predicate.test(file)) {
          consumer.accept(file.getPath().toString());
        }
      }

      if (subDirs.size() > maxDirectSubDirs) {
        remainingSubDirs.addAll(subDirs);
        return;
      }

      for (String subDir : subDirs) {
        processDirectoryWithHadoop(
            subDir,
            predicate,
            conf,
            pathFilter,
            consumer,
            maxDepth - 1,
            maxDirectSubDirs,
            remainingSubDirs);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
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
