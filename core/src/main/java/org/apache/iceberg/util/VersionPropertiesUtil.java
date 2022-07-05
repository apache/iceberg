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
import java.io.InputStream;
import java.util.Properties;
import org.apache.iceberg.relocated.com.google.common.io.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Loads the version.properties file with build information for project iceberg-core.
 */
public class VersionPropertiesUtil {

  private VersionPropertiesUtil() {
  }

  private static final Logger LOG = LoggerFactory.getLogger(VersionPropertiesUtil.class);
  private static final String VERSION_PROPERTIES_FILE = "version.properties";
  private static final String UNKNOWN_DEFAULT = "unknown";

  private static boolean isLoaded = false;
  private static Properties versionProperties = new Properties();

  private static String gitHash;  // 10 character short git hash of the build
  private static String gitHashFull;  // 40 character full git hash of the build
  private static String projectName;
  private static String projectVersion;

  /**
   * Loads the version.properties file for this module.
   */
  public static void loadBuildInfo() {
    try (InputStream is = readResource(VERSION_PROPERTIES_FILE)) {
      versionProperties.load(is);
    } catch (IOException e) {
      LOG.warn("Failed to load version properties from {}. Will use default values.", VERSION_PROPERTIES_FILE, e);
    }

    gitHash = versionProperties.getProperty("gitHash", UNKNOWN_DEFAULT);
    gitHashFull = versionProperties.getProperty("gitHashFull", UNKNOWN_DEFAULT);
    projectName = versionProperties.getProperty("projectName", UNKNOWN_DEFAULT);
    projectVersion = versionProperties.getProperty("projectVersion", UNKNOWN_DEFAULT);
  }

  public static String gitHash() {
    ensureLoaded();
    return gitHash;
  }

  public static String gitHashFull() {
    ensureLoaded();
    return gitHashFull;
  }

  public static String projectName() {
    ensureLoaded();
    return projectName;
  }

  public static String projectVersion() {
    ensureLoaded();
    return projectVersion;
  }

  private static void ensureLoaded() {
    if (!isLoaded) {
      synchronized (VersionPropertiesUtil.class) {
        if (!isLoaded) {
          loadBuildInfo();
          isLoaded = true;
        }
      }
    }
  }

  private static InputStream readResource(String resourceName) throws IOException {
    return Resources.asByteSource(Resources.getResource(VersionPropertiesUtil.class, resourceName)).openStream();
  }
}
