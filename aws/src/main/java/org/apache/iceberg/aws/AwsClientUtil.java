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

package org.apache.iceberg.aws;

import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * Provide factory methods to get default AWS clients.
 * The clients use a default {@link UrlConnectionHttpClient} to avoid multiple versions of AWS HTTP client builders
 * existing in the Java classpath, causing non-deterministic behavior of the AWS client.
 * The credential and region information are both loaded from the default chain.
 * For more details, see https://docs.aws.amazon.com/sdk-for-java/v2/developer-guide/credentials.html and
 * https://docs.aws.amazon.com/sdk-for-java/v2/developer-guide/java-dg-region-selection.html
 */
public class AwsClientUtil {

  private AwsClientUtil() {
  }

  public static S3Client defaultS3Client() {
    return S3Client.builder()
        .httpClient(UrlConnectionHttpClient.create())
        .build();
  }

  public static KmsClient defaultKmsClient() {
    return KmsClient.builder()
        .httpClient(UrlConnectionHttpClient.create())
        .build();
  }
}
