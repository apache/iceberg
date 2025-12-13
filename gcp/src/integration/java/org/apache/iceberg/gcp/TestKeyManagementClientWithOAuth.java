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
package org.apache.iceberg.gcp;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.OAuth2Credentials;
import com.google.cloud.kms.v1.KeyManagementServiceClient;
import com.google.cloud.kms.v1.KeyManagementServiceSettings;
import java.io.IOException;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariables;

@EnabledIfEnvironmentVariables({
  @EnabledIfEnvironmentVariable(
      named = TestKeyManagementClientWithOAuth.GOOGLE_OAUTH_TOKEN,
      matches = ".*"),
  @EnabledIfEnvironmentVariable(
      named = TestKeyManagementClientWithOAuth.GOOGLE_CLOUD_PROJECT_ID,
      matches = ".*")
})
public class TestKeyManagementClientWithOAuth extends TestKeyManagementClient {

  static final String GOOGLE_OAUTH_TOKEN = "GOOGLE_OAUTH_TOKEN";
  static final String GOOGLE_CLOUD_PROJECT_ID = "GOOGLE_CLOUD_PROJECT_ID";
  private String token;

  @Override
  protected void init() throws IOException {
    token = System.getenv(GOOGLE_OAUTH_TOKEN);
    AccessToken accessToken = new AccessToken(token, null);
    KeyManagementServiceSettings settings =
        KeyManagementServiceSettings.newBuilder()
            .setCredentialsProvider(
                FixedCredentialsProvider.create(OAuth2Credentials.create(accessToken)))
            .build();
    setKmsClient(KeyManagementServiceClient.create(settings));
    setProjectId(System.getenv(GOOGLE_CLOUD_PROJECT_ID));
  }

  @Override
  protected Map<String, String> properties() {
    return ImmutableMap.of(GCPProperties.GCS_OAUTH2_TOKEN, token);
  }
}
