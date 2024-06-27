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
package org.apache.iceberg;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.io.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Loads iceberg-version.properties with build information. */
public class IcebergBuild {
  private IcebergBuild() {}

  private static final Logger LOG = LoggerFactory.getLogger(IcebergBuild.class);
  private static final String VERSION_PROPERTIES_FILE = "/iceberg-build.properties";
  private static final String UNKNOWN_DEFAULT = "unknown";

  private static volatile boolean isLoaded = false;

  private static String shortId; // 10 character short git hash of the build
  private static String commitId; // 40 character full git hash of the build
  private static String branch;
  private static List<String> tags;
  private static String version;
  private static String fullVersion;

  /** Loads the version.properties file for this module. */
  @SuppressWarnings("CatchBlockLogException")
  public static void loadBuildInfo() {
    Properties buildProperties = new Properties();
    try (InputStream is = readResource(VERSION_PROPERTIES_FILE)) {
      buildProperties.load(is);
    } catch (Exception e) {
      LOG.warn("Failed to load version properties from {} due to {}", VERSION_PROPERTIES_FILE, e);
    }

    IcebergBuild.shortId = buildProperties.getProperty("git.commit.id.abbrev", UNKNOWN_DEFAULT);
    IcebergBuild.commitId = buildProperties.getProperty("git.commit.id", UNKNOWN_DEFAULT);
    IcebergBuild.branch = buildProperties.getProperty("git.branch", UNKNOWN_DEFAULT);
    String tagList = buildProperties.getProperty("git.tags", "");
    if (!tagList.isEmpty()) {
      IcebergBuild.tags = ImmutableList.copyOf(Splitter.on(",").split(tagList));
    } else {
      IcebergBuild.tags = ImmutableList.of();
    }
    IcebergBuild.version = buildProperties.getProperty("git.build.version", UNKNOWN_DEFAULT);
    IcebergBuild.fullVersion = String.format("Apache Iceberg %s (commit %s)", version, commitId);
  }

  public static String gitCommitId() {
    ensureLoaded();
    return commitId;
  }

  public static String gitCommitShortId() {
    ensureLoaded();
    return shortId;
  }

  public static String gitBranch() {
    ensureLoaded();
    return branch;
  }

  public static List<String> gitTags() {
    ensureLoaded();
    return tags;
  }

  public static String version() {
    ensureLoaded();
    return version;
  }

  public static String fullVersion() {
    ensureLoaded();
    return fullVersion;
  }

  private static void ensureLoaded() {
    if (!isLoaded) {
      synchronized (IcebergBuild.class) {
        if (!isLoaded) {
          loadBuildInfo();
          isLoaded = true;
        }
      }
    }
  }

  private static InputStream readResource(String resourceName) throws IOException {
    return Resources.asByteSource(Resources.getResource(IcebergBuild.class, resourceName))
        .openStream();
  }
}
