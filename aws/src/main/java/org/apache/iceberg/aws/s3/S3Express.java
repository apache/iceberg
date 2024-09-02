/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.aws.s3;

/**
 * Anything needed to support Amazon S3 Express One Zone Storage.
 * These have bucket names like {@code s3a://bucket--usw2-az2--x-s3/}
 */
public class S3Express {

  private S3Express() {
  }

  /**
   * Suffix of S3Express storage bucket names.
   */
  public static final String S3EXPRESS_STORE_SUFFIX = "--x-s3";

  /**
   * Check for a bucket name matching -does not look at endpoint.
   * @param bucket bucket to probe.
   * @return true if the suffix is present
   */
  public static boolean checkIfS3Express(final String bucket) {
    return bucket.endsWith(S3EXPRESS_STORE_SUFFIX);
  }
}
