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

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;

public class AwsClientPropertiesTest {

  @Test
  public void testDefaultCredentialsConfiguration() {
    AwsClientProperties awsClientProperties = new AwsClientProperties();
    AwsCredentialsProvider credentialsProvider =
        awsClientProperties.credentialsProvider(null, null, null);

    Assertions.assertThat(credentialsProvider instanceof DefaultCredentialsProvider)
        .withFailMessage("Should use default credentials if nothing is set")
        .isTrue();
  }

  @Test
  public void testBasicCredentialsConfiguration() {
    AwsClientProperties awsClientProperties = new AwsClientProperties();
    // set access key id and secret access key
    AwsCredentialsProvider credentialsProvider =
        awsClientProperties.credentialsProvider("key", "secret", null);

    Assertions.assertThat(credentialsProvider.resolveCredentials() instanceof AwsBasicCredentials)
        .withFailMessage(
            "Should use basic credentials if access key ID and secret access key are set")
        .isTrue();
    Assertions.assertThat(credentialsProvider.resolveCredentials().accessKeyId())
        .withFailMessage("The access key id should be the same as the one set by tag ACCESS_KEY_ID")
        .isEqualTo("key");

    Assertions.assertThat(credentialsProvider.resolveCredentials().secretAccessKey())
        .withFailMessage(
            "The secret access key should be the same as the one set by tag SECRET_ACCESS_KEY")
        .isEqualTo("secret");
  }

  @Test
  public void testSessionCredentialsConfiguration() {
    // set access key id, secret access key, and session token
    AwsClientProperties awsClientProperties = new AwsClientProperties();
    AwsCredentialsProvider credentialsProvider =
        awsClientProperties.credentialsProvider("key", "secret", "token");

    Assertions.assertThat(credentialsProvider.resolveCredentials() instanceof AwsSessionCredentials)
        .withFailMessage("Should use session credentials if session token is set")
        .isTrue();
    Assertions.assertThat(credentialsProvider.resolveCredentials().accessKeyId())
        .withFailMessage("The access key id should be the same as the one set by tag ACCESS_KEY_ID")
        .isEqualTo("key");
    Assertions.assertThat(credentialsProvider.resolveCredentials().secretAccessKey())
        .withFailMessage(
            "The secret access key should be the same as the one set by tag SECRET_ACCESS_KEY")
        .isEqualTo("secret");
  }
}
