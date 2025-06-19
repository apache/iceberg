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
}
