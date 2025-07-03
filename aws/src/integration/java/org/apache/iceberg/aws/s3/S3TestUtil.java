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
package org.apache.iceberg.aws.s3;

import static org.assertj.core.api.Assumptions.assumeThat;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.params.provider.Arguments;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3TestUtil {

  private static final Logger LOG = LoggerFactory.getLogger(S3TestUtil.class);

  private S3TestUtil() {}

  public static String getBucketFromUri(String s3Uri) {
    return new S3URI(s3Uri).bucket();
  }

  public static String getKeyFromUri(String s3Uri) {
    return new S3URI(s3Uri).key();
  }

  /**
   * Skip a test if the Analytics Accelerator Library for Amazon S3 is enabled.
   *
   * @param properties properties to probe
   */
  public static void skipIfAnalyticsAcceleratorEnabled(
      S3FileIOProperties properties, String message) {
    boolean isAcceleratorEnabled = properties.isS3AnalyticsAcceleratorEnabled();
    if (isAcceleratorEnabled) {
      LOG.warn(message);
    }
    assumeThat(!isAcceleratorEnabled).describedAs(message).isTrue();
  }

  public static Stream<Arguments> analyticsAcceleratorLibraryProperties() {
    return listAnalyticsAcceleratorLibraryProperties().stream().map(Arguments::of);
  }

  public static List<Map<String, String>> listAnalyticsAcceleratorLibraryProperties() {
    return List.of(
        ImmutableMap.of(
            S3FileIOProperties.S3_ANALYTICS_ACCELERATOR_ENABLED, Boolean.toString(true)),
        ImmutableMap.of(
            S3FileIOProperties.S3_ANALYTICS_ACCELERATOR_ENABLED, Boolean.toString(false)));
  }

  public static Map<String, String> mergeProperties(
      Map<String, String> aalProperties, Map<String, String> testProperties) {
    return ImmutableMap.<String, String>builder()
        .putAll(aalProperties)
        .putAll(testProperties)
        .build();
  }
}
