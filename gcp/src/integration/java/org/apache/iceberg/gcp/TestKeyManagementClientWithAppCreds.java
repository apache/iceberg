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

import static org.assertj.core.api.Assumptions.assumeThat;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.kms.v1.KeyManagementServiceClient;
import java.io.IOException;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariables;

@EnabledIfEnvironmentVariables({
  @EnabledIfEnvironmentVariable(named = "GOOGLE_APPLICATION_CREDENTIALS", matches = ".*")
})
public class TestKeyManagementClientWithAppCreds extends TestKeyManagementClient {

  @Override
  protected void init() throws IOException {
    GoogleCredentials credentials = GoogleCredentials.getApplicationDefault();
    assumeThat(credentials instanceof ServiceAccountCredentials).isTrue();

    setKmsClient(KeyManagementServiceClient.create());
    setProjectId(((ServiceAccountCredentials) credentials).getProjectId());
  }

  @Override
  protected Map<String, String> properties() {
    return ImmutableMap.of();
  }
}
