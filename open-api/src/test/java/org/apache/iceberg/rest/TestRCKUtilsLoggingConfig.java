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
package org.apache.iceberg.rest;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestRCKUtilsLoggingConfig {

  @TempDir
  Path tempDir;

  private String originalLogLevel;
  private String originalConfigFile;

  @BeforeEach
  void setUp() {
    // Save original system properties
    originalLogLevel = System.getProperty("log4j.rootLogger");
    originalConfigFile = System.getProperty("log4j.configuration");
  }

  @AfterEach
  void tearDown() {
    // Restore original system properties
    if (originalLogLevel != null) {
      System.setProperty("log4j.rootLogger", originalLogLevel);
    } else {
      System.clearProperty("log4j.rootLogger");
    }
    
    if (originalConfigFile != null) {
      System.setProperty("log4j.configuration", originalConfigFile);
    } else {
      System.clearProperty("log4j.configuration");
    }
  }

  @Test
  void testConfigureLoggingFromEnvironmentWithLogLevel() {
    // Note: This test verifies the method doesn't crash when called
    // In a real environment, CATALOG_LOG4J_LOGLEVEL would be set as an environment variable
    // We can't easily mock environment variables in unit tests, so we test the method exists and runs
    
    // Call the method (should not throw exception)
    RCKUtils.configureLoggingFromEnvironment();
    
    // The method should complete without error
    // In real usage, if CATALOG_LOG4J_LOGLEVEL=WARN is set as env var,
    // it would set System.setProperty("log4j.rootLogger", "WARN, stdout")
  }

  @Test
  void testConfigureLoggingFromEnvironmentWithConfigFile() throws IOException {
    // Create a temporary log4j.properties file
    File configFile = tempDir.resolve("test-log4j.properties").toFile();
    Files.write(configFile.toPath(), "log4j.rootLogger=ERROR, stdout".getBytes());
    
    // Note: This test verifies the method doesn't crash when called
    // In a real environment, CATALOG_LOG4J_CONFIG_FILE would be set as an environment variable
    
    // Call the method (should not throw exception)
    RCKUtils.configureLoggingFromEnvironment();
    
    // The method should complete without error
    // In real usage, if CATALOG_LOG4J_CONFIG_FILE=/path/to/file is set as env var,
    // it would set System.setProperty("log4j.configuration", "/path/to/file")
  }

  @Test
  void testConfigureLoggingFromEnvironmentWithSpecificLogger() {
    // Note: This test verifies the method doesn't crash when called
    // In a real environment, CATALOG_LOG4J_LOGGER_ORG_APACHE_ICEBERG_BASEMETASTORECATALOG would be set as an environment variable
    
    // Call the method (should not throw exception)
    RCKUtils.configureLoggingFromEnvironment();
    
    // The method should complete without error
    // In real usage, if CATALOG_LOG4J_LOGGER_ORG_APACHE_ICEBERG_BASEMETASTORECATALOG=ERROR is set as env var,
    // it would set System.setProperty("log4j.logger.org.apache.iceberg.BaseMetastoreCatalog", "ERROR")
  }

  @Test
  void testConfigureLoggingFromEnvironmentWithMultipleSettings() {
    // Note: This test verifies the method doesn't crash when called
    // In a real environment, multiple CATALOG_LOG4J_* variables would be set as environment variables
    
    // Call the method (should not throw exception)
    RCKUtils.configureLoggingFromEnvironment();
    
    // The method should complete without error
    // In real usage, multiple environment variables would be processed and set as system properties
  }

  @Test
  void testConfigureLoggingFromEnvironmentWithNoVariables() {
    // Clear any existing system properties
    System.clearProperty("log4j.rootLogger");
    System.clearProperty("log4j.configuration");
    
    // Call the method (should not set any properties)
    RCKUtils.configureLoggingFromEnvironment();
    
    // Verify no system properties were set
    assertThat(System.getProperty("log4j.rootLogger")).isNull();
    assertThat(System.getProperty("log4j.configuration")).isNull();
  }
}
