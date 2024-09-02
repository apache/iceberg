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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestS3Express {
  private static final String S3_GENERAL_BUCKET = "bucket";
  private static final String S3_EXPRESS_BUCKET_NAME = "express-bucket--usw2-az1--x-s3";

  @Test
  public void testS3ExpressStateAwsRegions() {
    assertS3ExpressState(S3_EXPRESS_BUCKET_NAME, true);
    assertS3ExpressState(S3_GENERAL_BUCKET, false);
  }

  public void assertS3ExpressState(final String bucket, final boolean expected) {
    assertThat(S3Express.checkIfS3Express(bucket))
            .describedAs("checkIfS3Express(%s)", bucket)
            .isEqualTo(expected);
  }
}
