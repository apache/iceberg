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
package org.apache.iceberg.spark.actions;

import static org.apache.iceberg.spark.actions.RewriteTablePathSparkAction.NOT_APPLICABLE;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.iceberg.RewriteTablePathUtil;
import org.apache.iceberg.actions.RewriteTablePath;
import org.apache.iceberg.util.Pair;
import org.assertj.core.api.AbstractAssert;

class RewriteTablePathResultAssert
    extends AbstractAssert<RewriteTablePathResultAssert, RewriteTablePath.Result> {

  private enum FileKind {
    VERSION_FILE,
    MANIFEST_LIST,
    MANIFEST,
    STATISTICS_FILE,
    PARTITION_STATISTICS_FILE,
    CONTENT_FILE;

    // manifest name ends up with either "-m0.parquet" or "m1.avro"
    private static final Pattern MANIFEST_NAME = Pattern.compile(".+-m\\d+\\..+");

    private static FileKind of(String location) {
      String name = RewriteTablePathUtil.fileName(location);
      if (name.endsWith(".metadata.json")) {
        return VERSION_FILE;
      } else if (name.startsWith("snap-")) {
        return MANIFEST_LIST;
      } else if (name.startsWith("optimized-m-") || MANIFEST_NAME.matcher(name).matches()) {
        return MANIFEST;
      } else if (name.startsWith("partition-stats-")) {
        return PARTITION_STATISTICS_FILE;
      } else if (name.endsWith(".stats")) {
        return STATISTICS_FILE;
      }

      return CONTENT_FILE;
    }
  }

  private final Set<FileKind> checkedKinds = EnumSet.noneOf(FileKind.class);
  private Map<FileKind, List<Pair<String, String>>> filesByKind;

  RewriteTablePathResultAssert(RewriteTablePath.Result actual) {
    super(actual, RewriteTablePathResultAssert.class);
  }

  static RewriteTablePathResultAssert assertThat(RewriteTablePath.Result actual) {
    return new RewriteTablePathResultAssert(actual);
  }

  RewriteTablePathResultAssert hasLatestVersion(String expected) {
    isNotNull();
    if (!Objects.equals(actual.latestVersion(), expected)) {
      failWithMessage("Expected latest version %s but was %s", expected, actual.latestVersion());
    }

    return myself;
  }

  RewriteTablePathResultAssert hasRewrittenDeleteFilePaths(int expected) {
    isNotNull();
    if (actual.rewrittenDeleteFilePathsCount() != expected) {
      failWithMessage(
          "Expected %s rewritten delete file paths but was %s",
          expected, actual.rewrittenDeleteFilePathsCount());
    }

    return myself;
  }

  RewriteTablePathResultAssert hasNoFileList() {
    isNotNull();
    if (!NOT_APPLICABLE.equals(actual.fileListLocation())) {
      failWithMessage("Expected no file list but found %s", actual.fileListLocation());
    }

    return myself;
  }

  RewriteTablePathResultAssert hasVersionFiles(int expected) {
    return hasFiles(FileKind.VERSION_FILE, expected);
  }

  RewriteTablePathResultAssert hasManifestLists(int expected) {
    return hasFiles(FileKind.MANIFEST_LIST, expected);
  }

  RewriteTablePathResultAssert hasManifests(int expected) {
    return hasFiles(FileKind.MANIFEST, expected);
  }

  RewriteTablePathResultAssert hasStatisticsFiles(int expected) {
    return hasFiles(FileKind.STATISTICS_FILE, expected);
  }

  RewriteTablePathResultAssert hasPartitionStatisticsFiles(int expected) {
    return hasFiles(FileKind.PARTITION_STATISTICS_FILE, expected);
  }

  RewriteTablePathResultAssert hasContentFiles(int expected) {
    return hasFiles(FileKind.CONTENT_FILE, expected);
  }

  RewriteTablePathResultAssert hasNoOtherFiles() {
    isNotNull();
    Map<FileKind, List<String>> unchecked = new EnumMap<>(FileKind.class);
    filesByKind()
        .forEach(
            (kind, files) -> {
              if (!checkedKinds.contains(kind)) {
                unchecked.put(kind, fileNames(files));
              }
            });
    if (!unchecked.isEmpty()) {
      failWithMessage(
          "Expected no files other than %s in %s but found:%n%s",
          checkedKinds, actual.fileListLocation(), unchecked);
    }

    return myself;
  }

  RewriteTablePathResultAssert hasStatisticsFilesCopiedAsIs(
      String sourcePrefix, String targetPrefix) {
    return hasFilesCopiedAsIs(FileKind.STATISTICS_FILE, sourcePrefix, targetPrefix);
  }

  RewriteTablePathResultAssert hasPartitionStatisticsFilesCopiedAsIs(
      String sourcePrefix, String targetPrefix) {
    return hasFilesCopiedAsIs(FileKind.PARTITION_STATISTICS_FILE, sourcePrefix, targetPrefix);
  }

  private RewriteTablePathResultAssert hasFiles(FileKind kind, int expected) {
    isNotNull();
    checkedKinds.add(kind);
    List<Pair<String, String>> files = filesByKind().getOrDefault(kind, List.of());
    if (files.size() != expected) {
      failWithMessage(
          "Expected %s %s in %s but found %s, counts by kind: %s%n%s: %s",
          expected,
          kind,
          actual.fileListLocation(),
          files.size(),
          countsByKind(),
          kind,
          fileNames(files));
    }

    return myself;
  }

  private Map<FileKind, Integer> countsByKind() {
    Map<FileKind, Integer> counts = new EnumMap<>(FileKind.class);
    filesByKind().forEach((kind, files) -> counts.put(kind, files.size()));
    return counts;
  }

  private static List<String> fileNames(List<Pair<String, String>> files) {
    return files.stream()
        .map(file -> RewriteTablePathUtil.fileName(file.second()))
        .collect(Collectors.toList());
  }

  private RewriteTablePathResultAssert hasFilesCopiedAsIs(
      FileKind kind, String sourcePrefix, String targetPrefix) {
    isNotNull();
    List<Pair<String, String>> files = filesByKind().getOrDefault(kind, List.of());
    if (files.isEmpty()) {
      failWithMessage("Expected %s in %s but found none", kind, actual.fileListLocation());
    }

    List<Pair<String, String>> notCopiedAsIs =
        files.stream()
            .filter(
                file ->
                    !file.first().startsWith(sourcePrefix)
                        || !RewriteTablePathUtil.newPath(file.first(), sourcePrefix, targetPrefix)
                            .equals(file.second()))
            .collect(Collectors.toList());
    if (!notCopiedAsIs.isEmpty()) {
      failWithMessage(
          "Expected every %s to be copied from %s to the same relative path under %s"
              + " but found (source, target):%n%s",
          kind, sourcePrefix, targetPrefix, notCopiedAsIs);
    }

    return myself;
  }

  private Map<FileKind, List<Pair<String, String>>> filesByKind() {
    if (filesByKind == null) {
      try {
        this.filesByKind =
            Files.readAllLines(Paths.get(URI.create(actual.fileListLocation()))).stream()
                .map(RewriteTablePathResultAssert::parseLine)
                .collect(
                    Collectors.groupingBy(
                        file -> FileKind.of(file.second()),
                        () -> new EnumMap<>(FileKind.class),
                        Collectors.toList()));
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    return filesByKind;
  }

  private static Pair<String, String> parseLine(String line) {
    int separator = line.lastIndexOf(',');
    return Pair.of(line.substring(0, separator), line.substring(separator + 1));
  }
}
